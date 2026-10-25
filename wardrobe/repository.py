"""SQLite 持久层：事务、世代化授权、逐人授权、幂等账本、审计与迁移执行。

整改要点（R2/R3/R6/R10）：
- 单连接全部事务经 `_tx_lock` 串行化，杜绝嵌套 BEGIN 与跨请求回滚互扰；
  长解码/下载不持有该锁。
- `auth_events` AUTOINCREMENT 单调序列实现授权世代：撤销/重授/清空/重绑定
  都推进 MAX(event_id)，旧 TurnScope 永不复活。
- `verify_commit` 在**最终写事务内部**复核 persona→role 绑定、role_id、
  世代、窗口授权 revision 与逐 sender 权限，与业务变更不可分离。
- 穿搭替换、条目删除（含强制）、配额检查、幂等账本认领全部单事务完成。
- 迁移在单事务脚本中执行；迁移前用 SQLite 在线 backup 生成一致性备份；
  start 失败关闭连接并复位状态，重试会重新检查 schema。
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite

from .config import WardrobeConfig
from .models import (
    BindingExists,
    InvalidInput,
    IdempotencyConflict,
    ImageRejected,
    PermissionDenied,
    SchemaTooNew,
    StaleContext,
    TurnScope,
    TxOutcomeUnknown,
    VersionConflict,
    WardrobeItem,
    utc_now_iso,
)
from .migrations import KNOWN_SCHEMA_VERSION, MIGRATIONS, pending_versions

_SCHEMA_META_NS = "schema_version"
_NAMESPACE_KEY = "installation_namespace"

# 迁移脚本内的受控字面量（G7）：version 推进与 DDL 同一提交单元；
# v2 追加旧作者连续性审计（created_at 由 SQLite 端生成，保持全字面量）。
_VERSION_META_SQL_TEMPLATE = (
    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', '{version}')"
    " ON CONFLICT(key) DO UPDATE SET value=excluded.value;"
)
_V2_META_AND_AUDIT_SQL = (
    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2')"
    " ON CONFLICT(key) DO UPDATE SET value=excluded.value;"
    " INSERT INTO audit_events(role_id, action, actor_ref, detail_json,"
    " result, created_at) VALUES (NULL, 'migrate_v2', NULL,"
    ' \'{"author_mapping_unrecoverable": true}\', \'ok\','
    " strftime('%Y-%m-%dT%H:%M:%fZ','now'));"
)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


async def tx_state(db: aiosqlite.Connection) -> bool | None:
    """严格事务状态（I2/I5）：读取失败返回 None——绝不把状态不可知
    伪装成 False/未提交；调用方必须把 None 当 unknown 结算。"""
    try:
        return bool(db.in_transaction)
    except Exception:
        return None


def split_sql_statements(script: str) -> list[str]:
    """把迁移脚本按 `;` 拆分为可逐条执行的语句。

    仅支持本仓库受控字面量脚本：剥离 `--` 注释行；字符串字面量中不出现
    分号（v1/v2 DDL 均满足，若未来脚本引入需扩展解析器并补测试）。
    """
    lines: list[str] = []
    for raw_line in script.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(raw_line)
    cleaned = "\n".join(lines)
    statements = [s.strip() for s in cleaned.split(";")]
    return [s for s in statements if s]


def _row_to_item(row: aiosqlite.Row) -> WardrobeItem:
    return WardrobeItem(
        item_id=row["item_id"],
        role_id=row["role_id"],
        name=row["name"],
        description=row["description"],
        category=row["category"],
        tags=json.loads(row["tags_json"]),
        layerable=bool(row["layerable"]),
        source_kind=row["source_kind"],
        created_by_ref=row["created_by_ref"],
        active=bool(row["active"]),
        revision=row["revision"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class Repository:
    """单连接仓储。启动于 start()，必须 stop() 释放；事务经互斥锁串行。"""

    def __init__(self, db_path: Path, config: WardrobeConfig) -> None:
        self.db_path = Path(db_path)
        self.config = config
        self._db: aiosqlite.Connection | None = None
        self.namespace: str | None = None
        self._tx_lock = asyncio.Lock()
        # I2/I3/I4：结算所有者登记（stop/settle_all 必须等待真实完成）
        self._settlement_owners: set[asyncio.Task] = set()
        self._broken = False

    # ------------------------------------------------------------------
    # 生命周期（R10）
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._db is not None:
            return
        self._broken = False
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        db = await aiosqlite.connect(self.db_path)
        try:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute("PRAGMA busy_timeout=5000")
            await db.commit()
            self._db = db
            await self._apply_migrations()
            # H8：任意 schema 版本（含无待执行迁移的最新版）启动都核验
            # 身份元数据——缺失则幂等补齐并审计；补齐失败则拒绝写入
            # （fail closed），绝不以共享 fallback namespace 继续运行
            await self._ensure_namespace(db)
            self.namespace = await self._get_meta(_NAMESPACE_KEY)
            if not self.namespace:
                raise RuntimeError(
                    "installation_namespace 缺失且无法恢复，已禁写")
        except BaseException:
            # 启动失败必须关闭连接并复位状态，重试才能重新检查 schema
            self._db = None
            self.namespace = None
            try:
                await db.close()
            except Exception:
                pass
            raise

    async def stop(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    # ------------------------------------------------------------------
    # 结算所有者（I2/I3/I4）：提交/回滚/坏连接处置的真实完成者
    # ------------------------------------------------------------------

    def _owned(self, coro) -> asyncio.Task:
        """登记结算所有者任务：等待者取消不影响其完成，stop 必须等待。"""
        task = asyncio.ensure_future(coro)
        self._settlement_owners.add(task)
        task.add_done_callback(self._settlement_owners.discard)
        return task

    async def settle_all(self) -> None:
        """等待全部结算所有者真实完成（DB 关闭必须在其之后；I2.6）。"""
        while self._settlement_owners:
            await asyncio.wait(list(self._settlement_owners))

    async def _quarantine_broken_connection(self) -> None:
        """回滚不可靠完成/状态不可读后的坏连接处置（I3）：关闭并禁写。"""
        self._broken = True
        db, self._db = self._db, None
        if db is not None:
            try:
                await db.close()
            except Exception:
                pass

    @property
    def is_open(self) -> bool:
        return self._db is not None

    def _require(self) -> aiosqlite.Connection:
        if self._broken:
            raise RuntimeError("事务连接已损坏并停用；请重启插件恢复服务")
        if self._db is None:
            raise RuntimeError("repository 未启动或已关闭")
        return self._db

    # ------------------------------------------------------------------
    # 迁移（R10：原子执行 + 一致性备份）
    # ------------------------------------------------------------------

    async def _get_meta(self, key: str) -> str | None:
        db = self._require()
        cur = await db.execute("SELECT value FROM schema_meta WHERE key=?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def _apply_migrations(self) -> None:
        db = self._require()
        cur = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        )
        exists = await cur.fetchone()
        current = 0
        if exists:
            value = await self._get_meta(_SCHEMA_META_NS)
            if value is not None:
                current = int(value)
        if current > KNOWN_SCHEMA_VERSION:
            # 每次启动都拒写：不修改原库，只抛出受控错误
            raise SchemaTooNew(
                f"数据库 schema 版本 {current} 高于插件支持的 {KNOWN_SCHEMA_VERSION}，"
                "请使用对应版本插件；已拒绝写入。")
        for version in pending_versions(current):
            if version != current + 1:
                raise RuntimeError(f"迁移版本不连续：当前 {current}，下一个 {version}")
            if current >= 1:
                # v0 空库无数据可备份；v1+ 升级前必须一致性备份
                await self._backup_before_migrate(current)
            # G7：DDL 与 schema_version 推进（v1→v2 含旧作者连续性审计）在
            # 同一脚本内提交（executescript 脚本自身 BEGIN...COMMIT 为一个
            # 真实提交单元，不存在“DDL 成功而 version 未推进”的半升级）。
            # installation_namespace 由 start() 末尾的 _ensure_namespace
            # 无条件核验/补齐（H8），不依赖本循环。
            if version == 2:
                script = f"BEGIN IMMEDIATE;\n{MIGRATIONS[version]}\n{_V2_META_AND_AUDIT_SQL}\nCOMMIT;"
            else:
                script = f"BEGIN IMMEDIATE;\n{MIGRATIONS[version]}\n{_VERSION_META_SQL_TEMPLATE.format(version=version)}\nCOMMIT;"
            try:
                await db.executescript(script)
            except Exception:
                # 脚本内 BEGIN 后失败：回滚保持一致旧 schema 并禁写（抛出）
                try:
                    await db.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            current = version

    async def _ensure_namespace(self, db: aiosqlite.Connection) -> None:
        """installation_namespace 无条件核验与幂等补齐（H8）。

        只写缺失值，绝不覆盖既有 namespace；补齐与审计同一事务。写入
        失败由调用方（start）关闭连接并禁写——故障后重启不允许以
        namespace=None/共享回退继续运行。
        """
        value = await self._get_meta(_NAMESPACE_KEY)
        if value:
            return
        async with self.tx() as conn:
            await conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES (?, ?)",
                (_NAMESPACE_KEY, uuid.uuid4().hex),
            )
            await conn.execute(
                "INSERT INTO audit_events(role_id, action, actor_ref,"
                " detail_json, result, created_at)"
                " VALUES (NULL, 'namespace_recovered', NULL, '{}', 'ok', ?)",
                (utc_now_iso(),))

    async def _backup_before_migrate(self, from_version: int) -> None:
        """迁移前一致性备份：SQLite 在线 backup API（含 WAL 内容，带一致性快照）。"""
        backup_dir = self.db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = utc_now_iso().replace(":", "").replace("-", "")
        target_path = backup_dir / f"pre-migrate-v{from_version}-{stamp}.db"
        source = self._require()
        dest = sqlite3.connect(target_path)
        try:
            await source.backup(dest)
        finally:
            dest.close()
        # 核对备份可打开且关键表齐全（v1+ 升级前应已具备核心表）
        verify = sqlite3.connect(target_path)
        try:
            row = verify.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                " AND name IN ('schema_meta','roles','wardrobe_items',"
                " 'image_assets')").fetchone()
            if not row or row[0] < 4 or target_path.stat().st_size == 0:
                raise RuntimeError("迁移备份不完整，已终止迁移")
        finally:
            verify.close()

    # ------------------------------------------------------------------
    # 事务（R6：互斥锁串行）
    # ------------------------------------------------------------------

    class _Tx:
        """单连接事务：互斥锁串行 + 宿主核验钩子 + 三态结算所有者（I2/I3）。

        结算三态（settlement outcome）：
        - committed      真实 COMMIT 已完成；
        - not_committed  确定未提交（业务异常/提交前失败，且回滚真实成功）；
        - unknown        回滚失败或状态读取失败——提交结果无法确认。
        结算（提交/回滚/锁归还/坏连接停用）由独立结算所有者任务执行，
        等待者被一次或多次取消只会继续等待真实完成：不跳过回滚、不提前
        归还锁、也不把 CancelledError 当作"未提交"证据：
        - 正常路径：committed → 正常返回；not_committed → 传播原异常；
          unknown → 抛 TxOutcomeUnknown；
        - 取消与结算竞争：not_committed → 传播原异常（可安全补偿文件）；
          committed/unknown → 抛 TxOutcomeUnknown（文件保留，由 /衣柜 核对
          或幂等重放受控恢复结算）。
        准入失败（I3）：BEGIN 真实完成后挂起、BEGIN 后宿主钩子失败两条
        路径都由独立所有者结算（真实回滚 → 锁归还；回滚不可靠完成 →
        停用坏连接，不让其继续服务）。
        """

        def __init__(self, repo: "Repository", lock: asyncio.Lock,
                     pre_commit=None) -> None:
            self._repo = repo
            self.db = repo._require()
            self.lock = lock
            self.pre_commit = pre_commit
            self._locked = False
            self.outcome = "pending"   # pending|committed|not_committed|unknown
            self._commit_exc: BaseException | None = None
            self._done = asyncio.Event()
            # J1：BEGIN 自身的真实执行归属——aiosqlite 把 SQL 放入唯一工作
            # 线程队列后返回 future；等待者取消不取消已入队工作。结算必须
            # 等待该 future 的真实结果，不得以观察到的 in_transaction=False
            # 当作"BEGIN 不会开始"的依据。
            self._begin_task: asyncio.Task | None = None

        async def __aenter__(self) -> aiosqlite.Connection:
            await self.lock.acquire()
            self._locked = True
            self._begin_task = asyncio.ensure_future(
                self.db.execute("BEGIN IMMEDIATE"))
            try:
                await asyncio.shield(self._begin_task)
            except BaseException as begin_exc:
                # 真实 BEGIN 可能已执行/仍在队列/真正失败：owned 所有者
                # 先等待 BEGIN 真实结果，再回滚或停用连接，最后归还锁
                await self._settle_entry_failure(begin_exc)
                raise
            try:
                if self.pre_commit is not None:
                    await self.pre_commit()
            except BaseException as hook_exc:
                await self._settle_entry_failure(hook_exc)
                raise
            return self.db

        async def _settle_entry_failure(self, exc: BaseException) -> None:
            """准入失败结算（I3/J1）：owned 所有者掌握真实 BEGIN 结果后
            回滚/停用并归还锁。

            等待者未被取消时等待真实完成（回滚先于调用方感知异常）；
            等待者被取消（含再次/多次取消）时所有者继续后台完成，
            绝不跳过 BEGIN 结果等待、回滚与锁释放。
            """
            owner = self._repo._owned(self._entry_settlement())
            while not self._done.is_set():
                try:
                    await asyncio.shield(owner)
                except asyncio.CancelledError:
                    continue
            if isinstance(exc, asyncio.CancelledError):
                raise exc

        async def _entry_settlement(self) -> None:
            try:
                begin_failed = False
                if self._begin_task is not None:
                    # J1：先取 BEGIN 的真实执行结果（队列中的工作一定会
                    # 被唯一线程执行；等待者取消不影响该 future）
                    try:
                        await self._begin_task
                    except asyncio.CancelledError:
                        # begin_task 自身不会被取消（shield 保护）；防御
                        raise
                    except Exception:
                        begin_failed = True  # 真实 SQL 失败：事务未开始
                state = await tx_state(self.db)
                if begin_failed and state is False:
                    # BEGIN 真正未成功：无需回滚，锁可直接归还
                    self.outcome = "not_committed"
                    return
                if state is True:
                    try:
                        await self.db.rollback()
                        self.outcome = "not_committed"
                    except Exception:
                        # 回滚不可靠完成：停用坏连接，不让其继续服务
                        self.outcome = "unknown"
                        await self._repo._quarantine_broken_connection()
                elif state is None:
                    self.outcome = "unknown"
                    await self._repo._quarantine_broken_connection()
                elif begin_failed:
                    # BEGIN 失败但连接竟在事务中：状态矛盾，按未知停用
                    self.outcome = "unknown"
                    await self._repo._quarantine_broken_connection()
                else:
                    # BEGIN 成功且无异常路径到达这里（防御）：回滚后归还
                    try:
                        await self.db.rollback()
                        self.outcome = "not_committed"
                    except Exception:
                        self.outcome = "unknown"
                        await self._repo._quarantine_broken_connection()
            finally:
                if self._locked:
                    self.lock.release()
                    self._locked = False
                self._done.set()

        async def __aexit__(self, exc_type, exc, tb) -> None:
            owner = self._repo._owned(self._finish(exc))
            while not self._done.is_set():
                try:
                    await asyncio.shield(owner)
                except asyncio.CancelledError:
                    # 等待者取消（一次或多次）：结算所有者继续真实完成；
                    # 等待者不得把取消传播给所有者，也不得抢跑三态映射
                    continue
            if self.outcome == "not_committed":
                # 确定未提交：传播原异常，调用方按未提交补偿本请求文件
                if exc is not None:
                    raise exc
                raise self._commit_exc  # type: ignore[misc]
            if self.outcome == "committed":
                return
            raise TxOutcomeUnknown(
                "提交结果无法确认；不得按未提交删除文件，"
                "效果以 /衣柜 核对 或幂等重放受控恢复") from exc

        async def _finish(self, exc) -> None:
            try:
                if exc is None:
                    try:
                        await self.db.commit()
                        self.outcome = "committed"
                    except BaseException as commit_exc:
                        self._commit_exc = commit_exc
                        state = await tx_state(self.db)
                        if state is True:
                            try:
                                await self.db.rollback()
                                self.outcome = "not_committed"
                            except Exception:
                                self.outcome = "unknown"
                                await self._repo._quarantine_broken_connection()
                        elif state is None:
                            self.outcome = "unknown"
                            await self._repo._quarantine_broken_connection()
                        else:
                            # 连接已不在事务中：COMMIT 可能实际完成 → unknown
                            self.outcome = "unknown"
                else:
                    state = await tx_state(self.db)
                    if state is True:
                        try:
                            await self.db.rollback()
                            self.outcome = "not_committed"
                        except Exception:
                            self.outcome = "unknown"
                            await self._repo._quarantine_broken_connection()
                    else:
                        # 回滚前已不在事务/状态不可读：无法确认提交结果
                        self.outcome = "unknown"
                        if state is None:
                            await self._repo._quarantine_broken_connection()
            finally:
                if self._locked:
                    self.lock.release()
                    self._locked = False
                self._done.set()

    def tx(self, pre_commit=None) -> "Repository._Tx":
        return Repository._Tx(self, self._tx_lock, pre_commit)

    # ------------------------------------------------------------------
    # 授权世代（R3）
    # ------------------------------------------------------------------

    async def auth_generation(self) -> int:
        db = self._require()
        cur = await db.execute("SELECT COALESCE(MAX(event_id), 0) AS g FROM auth_events")
        return (await cur.fetchone())["g"]

    async def _record_auth_event(self, conn: aiosqlite.Connection, role_id: str | None,
                                 kind: str) -> None:
        await conn.execute(
            "INSERT INTO auth_events(role_id, kind, created_at) VALUES (?,?,?)",
            (role_id, kind, utc_now_iso()))

    async def auth_generation_snapshot(self, role_id: str | None = None) -> int:
        """世代 + 绑定 revision 联合快照，供 TurnScope 捕获。"""
        return await self.auth_generation()

    # ------------------------------------------------------------------
    # 提交前复核（R3：在最终写事务内部执行）
    # ------------------------------------------------------------------

    async def verify_commit(self, conn: aiosqlite.Connection, scope: TurnScope,
                            persona_key: str | None) -> None:
        """在同一写事务内复核：绑定、role_id、世代、窗口授权、逐人权限。

        任何不满足抛 StaleContext；与调用方的业务变更同事务提交或回滚。
        """
        cur = await conn.execute(
            "SELECT b.role_id, b.binding_revision FROM persona_bindings b"
            " WHERE b.host_persona_key=?", (persona_key,))
        binding = await cur.fetchone()
        if binding is None or binding["role_id"] != scope.role_id:
            raise StaleContext("角色绑定已变化（重绑/清空），旧请求作废")
        if binding["binding_revision"] != scope.binding_revision:
            raise StaleContext("角色绑定 revision 已变化，旧请求作废")
        cur = await conn.execute("SELECT COALESCE(MAX(event_id), 0) AS g FROM auth_events")
        if (await cur.fetchone())["g"] != scope.auth_generation:
            raise StaleContext("授权世代已推进（撤销/重授/清空/重绑），旧请求作废")
        cur = await conn.execute(
            "SELECT revision FROM session_grants WHERE role_id=? AND umo_ref=?",
            (scope.role_id, scope.umo))
        grant = await cur.fetchone()
        if grant is None or grant["revision"] != scope.grant_revision:
            raise StaleContext("窗口授权已变化，旧请求作废")
        if not scope.is_host_admin:
            need = scope.minimum_level
            if need in ("contributor", "maintainer"):
                cur = await conn.execute(
                    "SELECT level FROM sender_grants"
                    " WHERE role_id=? AND umo_ref=? AND sender_ref=?",
                    (scope.role_id, scope.umo, scope.sender_ref))
                row = await cur.fetchone()
                have = row["level"] if row else "reader"
                order = {"reader": 0, "contributor": 1, "maintainer": 2}
                if order[have] < order[need]:
                    raise StaleContext("请求执行期间权限被回收，旧请求作废")

    # ------------------------------------------------------------------
    # 角色 / 绑定 / 授权
    # ------------------------------------------------------------------

    async def create_role(self, display_name: str, actor_ref: str,
                          host_persona_key: str, umo_ref: str,
                          pre_commit=None) -> tuple[str, int, int]:
        """初始化：建角色、绑定 persona、登记窗口、给发起者 maintainer 逐人授权。

        返回 (role_id, binding_revision, grant_revision)。
        """
        now = utc_now_iso()
        role_id = _new_id("role")
        async with self.tx(pre_commit) as conn:
            cur = await conn.execute(
                "SELECT role_id FROM persona_bindings WHERE host_persona_key=?",
                (host_persona_key,))
            if await cur.fetchone():
                raise BindingExists("该人格已绑定其他角色；迁移需走显式重绑定流程")
            await conn.execute(
                "INSERT INTO roles(role_id, display_name, created_at) VALUES (?,?,?)",
                (role_id, display_name, now))
            await conn.execute(
                "INSERT INTO persona_bindings(host_persona_key, role_id, bound_at,"
                " bound_by_ref) VALUES (?,?,?,?)",
                (host_persona_key, role_id, now, actor_ref))
            await conn.execute(
                "INSERT INTO session_grants(role_id, umo_ref, permission_level,"
                " allow_ai_mutation, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                (role_id, umo_ref, "reader", 1, now, now))
            await conn.execute(
                "INSERT INTO sender_grants(role_id, umo_ref, sender_ref, level,"
                " created_at, updated_at) VALUES (?,?,?,?,?,?)",
                (role_id, umo_ref, actor_ref, "maintainer", now, now))
            await conn.execute(
                "INSERT INTO outfit_state(role_id, updated_at) VALUES (?,?)",
                (role_id, now))
            await conn.execute(
                "INSERT INTO autonomy_state(role_id, add_enabled, wear_enabled)"
                " VALUES (?,?,?)",
                (role_id, 1 if self.config.autonomy_add_enabled else 0,
                 1 if self.config.autonomy_wear_enabled else 0))
            await self._record_auth_event(conn, role_id, "initialize")
        db = self._require()
        cur = await db.execute(
            "SELECT binding_revision FROM persona_bindings WHERE host_persona_key=?",
            (host_persona_key,))
        binding_rev = (await cur.fetchone())["binding_revision"]
        cur = await db.execute(
            "SELECT revision FROM session_grants WHERE role_id=? AND umo_ref=?",
            (role_id, umo_ref))
        grant_rev = (await cur.fetchone())["revision"]
        return role_id, binding_rev, grant_rev

    async def get_binding(self, host_persona_key: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT b.*, r.enabled AS role_enabled, r.display_name AS role_display,"
            " r.revision AS role_revision"
            " FROM persona_bindings b JOIN roles r ON r.role_id = b.role_id"
            " WHERE b.host_persona_key=?",
            (host_persona_key,))
        return await cur.fetchone()

    async def rebind_persona(self, host_persona_key: str, role_id: str,
                             actor_ref: str) -> int:
        """显式重绑定：revision 递增 + 推进授权世代，旧请求全部作废。"""
        db = self._require()
        now = utc_now_iso()
        new_revision = 1
        async with self.tx() as conn:
            cur = await conn.execute(
                "SELECT binding_revision FROM persona_bindings WHERE host_persona_key=?",
                (host_persona_key,))
            row = await cur.fetchone()
            if row:
                new_revision = row["binding_revision"] + 1
                await conn.execute(
                    "DELETE FROM persona_bindings WHERE host_persona_key=?",
                    (host_persona_key,))
            await conn.execute(
                "INSERT INTO persona_bindings(host_persona_key, role_id,"
                " binding_revision, bound_at, bound_by_ref) VALUES (?,?,?,?,?)",
                (host_persona_key, role_id, new_revision, now, actor_ref))
            await conn.execute(
                "UPDATE roles SET revision=revision+1 WHERE role_id=?", (role_id,))
            await self._record_auth_event(conn, role_id, "rebind")
        return new_revision

    async def get_grant(self, role_id: str, umo_ref: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM session_grants WHERE role_id=? AND umo_ref=?",
            (role_id, umo_ref))
        return await cur.fetchone()

    async def set_grant(self, role_id: str, umo_ref: str, allow_ai: bool,
                        scope: TurnScope | None = None,
                        persona_key: str | None = None,
                        pre_commit=None) -> int:
        """窗口启用/更新（窗口内成员默认 reader；逐人级别见 sender_grants）。

        G2：最终写事务内复核当前身份/绑定/世代/授权，scope 缺失时拒绝。
        H1：pre_commit 宿主核验钩子在事务准入后执行。
        """
        if scope is None:
            raise StaleContext("缺少本轮作用域，最终写事务拒绝执行")
        db = self._require()
        now = utc_now_iso()
        new_revision = 1
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            cur = await conn.execute(
                "SELECT revision FROM session_grants WHERE role_id=? AND umo_ref=?",
                (role_id, umo_ref))
            row = await cur.fetchone()
            if row:
                new_revision = row["revision"] + 1
                await conn.execute(
                    "UPDATE session_grants SET permission_level='reader',"
                    " allow_ai_mutation=?, revision=?, updated_at=?"
                    " WHERE role_id=? AND umo_ref=?",
                    (1 if allow_ai else 0, new_revision, now, role_id, umo_ref))
            else:
                await conn.execute(
                    "INSERT INTO session_grants(role_id, umo_ref, permission_level,"
                    " allow_ai_mutation, revision, created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (role_id, umo_ref, "reader", 1 if allow_ai else 0, 1, now, now))
            await self._record_auth_event(conn, role_id, "regrant")
        return new_revision

    async def revoke_grant(self, role_id: str, umo_ref: str,
                           scope: TurnScope | None = None,
                           persona_key: str | None = None,
                           pre_commit=None) -> bool:
        if scope is None:
            raise StaleContext("缺少本轮作用域，最终写事务拒绝执行")
        db = self._require()
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            cur = await conn.execute(
                "DELETE FROM session_grants WHERE role_id=? AND umo_ref=?",
                (role_id, umo_ref))
            removed = cur.rowcount > 0
            await conn.execute(
                "DELETE FROM sender_grants WHERE role_id=? AND umo_ref=?",
                (role_id, umo_ref))
            if removed:
                await self._record_auth_event(conn, role_id, "revoke")
        return removed

    async def get_sender_level(self, role_id: str, umo_ref: str,
                               sender_ref: str) -> str:
        db = self._require()
        cur = await db.execute(
            "SELECT level FROM sender_grants"
            " WHERE role_id=? AND umo_ref=? AND sender_ref=?",
            (role_id, umo_ref, sender_ref))
        row = await cur.fetchone()
        return row["level"] if row else "reader"

    async def set_sender_grant(self, role_id: str, umo_ref: str, sender_ref: str,
                               level: str | None,
                               scope: TurnScope | None = None,
                               persona_key: str | None = None,
                               pre_commit=None) -> None:
        """设置/移除逐人级别。level=None 表示降回 reader（删除行）。

        G2：INSERT/UPDATE 与 DELETE 分支共用同一最终事务复核。
        """
        if scope is None:
            raise StaleContext("缺少本轮作用域，最终写事务拒绝执行")
        db = self._require()
        now = utc_now_iso()
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            if level is None:
                await conn.execute(
                    "DELETE FROM sender_grants"
                    " WHERE role_id=? AND umo_ref=? AND sender_ref=?",
                    (role_id, umo_ref, sender_ref))
            else:
                await conn.execute(
                    "INSERT INTO sender_grants(role_id, umo_ref, sender_ref, level,"
                    " created_at, updated_at) VALUES (?,?,?,?,?,?)"
                    " ON CONFLICT(role_id, umo_ref, sender_ref)"
                    " DO UPDATE SET level=excluded.level, updated_at=excluded.updated_at",
                    (role_id, umo_ref, sender_ref, level, now, now))
            await self._record_auth_event(conn, role_id, "sender_grant")

    async def set_role_enabled(self, role_id: str, enabled: bool) -> None:
        db = self._require()
        async with self.tx() as conn:
            await conn.execute(
                "UPDATE roles SET enabled=?, revision=revision+1 WHERE role_id=?",
                (1 if enabled else 0, role_id))

    # ------------------------------------------------------------------
    # 服装（查询）
    # ------------------------------------------------------------------

    async def count_items(self, role_id: str) -> int:
        db = self._require()
        cur = await db.execute(
            "SELECT COUNT(*) AS n FROM wardrobe_items WHERE role_id=? AND active=1",
            (role_id,))
        return (await cur.fetchone())["n"]

    async def get_item(self, role_id: str, item_id: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM wardrobe_items WHERE role_id=? AND item_id=?",
            (role_id, item_id))
        return await cur.fetchone()

    async def list_items(self, role_id: str, page: int, category: str | None,
                         page_size: int) -> tuple[list[aiosqlite.Row], int]:
        db = self._require()
        where = "role_id=? AND active=1"
        params: list[object] = [role_id]
        if category:
            where += " AND category=?"
            params.append(category)
        cur = await db.execute(
            f"SELECT COUNT(*) AS n FROM wardrobe_items WHERE {where}", params)
        total = (await cur.fetchone())["n"]
        cur = await db.execute(
            f"SELECT * FROM wardrobe_items WHERE {where}"
            " ORDER BY created_at DESC, item_id DESC LIMIT ? OFFSET ?",
            [*params, page_size, (page - 1) * page_size])
        return list(await cur.fetchall()), total

    async def update_item_verified(self, scope: TurnScope,
                                   persona_key: str | None, role_id: str,
                                   item_id: str, fields: dict,
                                   pre_commit=None) -> int:
        """G2/G8：最终事务内复核权限 + 已穿衣服类别/叠穿编辑拒绝。

        返回新 revision。列名来自受控白名单；空更新直接返回当前 revision。
        """
        allowed = {"name", "description", "category", "tags_json", "layerable"}
        unknown = set(fields) - allowed
        if unknown:
            from .models import InvalidInput

            raise InvalidInput(f"未知字段：{'、'.join(sorted(unknown))}")
        db = self._require()
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            cur = await conn.execute(
                "SELECT active FROM wardrobe_items WHERE role_id=? AND item_id=?",
                (role_id, item_id))
            row = await cur.fetchone()
            if row is None or not row["active"]:
                raise LookupError("item 不存在或已删除")
            if fields.keys() & {"category", "layerable"}:
                cur = await conn.execute(
                    "SELECT item_id FROM outfit_items"
                    " WHERE role_id=? AND item_id=?",
                    (role_id, item_id))
                if await cur.fetchone():
                    from .models import PermissionDenied

                    raise PermissionDenied(
                        "该服装正在当前穿搭中；类别/可叠穿属性需先移出穿搭再修改")
            sets: list[str] = [f"{key}=?" for key in fields]
            if sets:
                sets.append("revision=revision+1")
                sets.append("updated_at=?")
                params: list[object] = [*fields.values(), utc_now_iso(),
                                        role_id, item_id]
                cur = await conn.execute(
                    f"UPDATE wardrobe_items SET {', '.join(sets)}"
                    " WHERE role_id=? AND item_id=? AND active=1",
                    params)
                if cur.rowcount != 1:
                    raise LookupError("item 不存在或已删除")
            cur = await conn.execute(
                "SELECT revision FROM wardrobe_items WHERE role_id=? AND item_id=?",
                (role_id, item_id))
            return (await cur.fetchone())["revision"]

    # ------------------------------------------------------------------
    # 原子写入：添加（R5/R6/R7）
    # ------------------------------------------------------------------

    async def insert_item_verified(
        self, scope: TurnScope, persona_key: str | None, item: WardrobeItem,
        new_assets: list[dict], request_key: str | None, fingerprint: str | None,
        pre_commit=None, autonomy_check=None,
    ) -> dict:
        """G3 单事务：复核 → 幂等认领 → 事务内去重重查 → 配额 → 条目/资产/全部引用/账本。

        new_assets 是**全部**本次处理完成的资产候选（含 prepare 阶段疑似命中
        旧图者——旧图可能已被合法删除，因此在事务内按 digest 重新判定）。
        同批相同 digest 只落一个 asset 行并共享引用；ordinal 保留真实顺序。
        H2：同批重复 digest 的后续候选同样列入 dropped——每份已发布候选
        文件都有归属，调用方据此即时清理，不残留不计配额的托管文件。
        H3：ledger 结果与首返同源同形（含 tags/asset_count/asset_ids），
        重放与首次成功深度一致（仅调用方附加 replay 标识）。
        返回 {status: committed|replayed, result, dropped_storage_keys}；
        dropped_storage_keys 是事务内未采用的新资产存储键（文件由调用方清理）。
        """
        db = self._require()
        async with self.tx(pre_commit) as conn:
            if request_key:
                cur = await conn.execute(
                    "SELECT action, result_ref, fingerprint FROM operation_ledger"
                    " WHERE role_id=? AND request_key=?",
                    (scope.role_id, request_key))
                prior = await cur.fetchone()
                if prior:
                    if fingerprint is not None and prior["fingerprint"] != fingerprint:
                        raise IdempotencyConflict(
                            "同一请求键已用于不同参数，已拒绝")
                    return {"status": "replayed",
                            "result": json.loads(prior["result_ref"] or "{}"),
                            "dropped_storage_keys": []}
            await self.verify_commit(conn, scope, persona_key)
            # M2：自主许可 gate（开关/窗口/turn 预算/日额度/冷却）原子核验
            # 与消耗——失败即整事务回滚，业务零副作用
            if autonomy_check is not None:
                await autonomy_check(conn)
            # 事务内去重重查：库中已有（同角色 ready）或本批已插入者共享资产行
            seen: dict[str, str] = {}
            to_insert: list[dict] = []
            dropped_keys: list[str] = []
            refs: list[tuple[str, int]] = []
            ref_assets: set[str] = set()
            for meta in new_assets:
                digest = meta["processed_sha256"]
                if digest not in seen:
                    cur = await conn.execute(
                        "SELECT asset_id FROM image_assets"
                        " WHERE role_id=? AND processed_sha256=? AND status='ready'",
                        (scope.role_id, digest))
                    row = await cur.fetchone()
                    if row:
                        seen[digest] = row["asset_id"]
                        dropped_keys.append(meta["storage_key"])
                    else:
                        seen[digest] = meta["asset_id"]
                        to_insert.append(meta)
                else:
                    # H2：同批重复 digest 的后续候选文件同样有归属——
                    # 登记为 dropped，由调用方在结算时删除
                    dropped_keys.append(meta["storage_key"])
                asset_id = seen[digest]
                # item_images 主键 (item_id, asset_id)：同一资产在同一条目
                # 只落一行引用（同批重复 hash 共享首次 ordinal）
                if asset_id in ref_assets:
                    continue
                ref_assets.add(asset_id)
                refs.append((asset_id, meta["ordinal"]))
            # 配额（件数 + 字节）按事务内实际将写入的资产核算
            cur = await conn.execute(
                "SELECT COUNT(*) AS n FROM wardrobe_items"
                " WHERE role_id=? AND active=1", (scope.role_id,))
            if (await cur.fetchone())["n"] >= self.config.max_items_per_role:
                from .models import QuotaExceeded

                raise QuotaExceeded(
                    f"衣柜已达 {self.config.max_items_per_role} 件上限")
            role_bytes = sum(asset["byte_size"] for asset in to_insert)
            cur = await conn.execute(
                "SELECT COALESCE(SUM(byte_size), 0) AS total FROM image_assets"
                " WHERE role_id=? AND status='ready'", (scope.role_id,))
            if ((await cur.fetchone())["total"] + role_bytes
                    > self.config.role_image_budget_bytes):
                from .models import ImageRejected

                raise ImageRejected("角色图片总额已满；请先清理或联系管理者")
            await conn.execute(
                "INSERT INTO wardrobe_items(item_id, role_id, name, description,"
                " category, tags_json, layerable, source_kind, created_by_ref,"
                " active, revision, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,1,1,?,?)",
                (item.item_id, scope.role_id, item.name, item.description,
                 item.category, json.dumps(item.tags, ensure_ascii=False),
                 1 if item.layerable else 0, item.source_kind,
                 item.created_by_ref, item.created_at, item.updated_at))
            for asset in to_insert:
                await conn.execute(
                    "INSERT INTO image_assets(asset_id, role_id, processed_sha256,"
                    " storage_key, mime, byte_size, width, height, auto_caption,"
                    " caption_state, status, created_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (asset["asset_id"], scope.role_id, asset["processed_sha256"],
                     asset["storage_key"], asset["mime"], asset["byte_size"],
                     asset["width"], asset["height"], asset.get("auto_caption"),
                     asset.get("caption_state", "none"), asset.get("status", "ready"),
                     asset["created_at"]))
            for asset_id, ordinal in refs:
                await conn.execute(
                    "INSERT INTO item_images(role_id, item_id, asset_id, ordinal)"
                    " VALUES (?,?,?,?)",
                    (scope.role_id, item.item_id, asset_id, ordinal))
            result = {"item_id": item.item_id, "name": item.name,
                      "category": item.category, "tags": list(item.tags),
                      "asset_count": len(refs),
                      "asset_ids": [asset_id for asset_id, _ in refs]}
            if request_key:
                await conn.execute(
                    "INSERT INTO operation_ledger(role_id, request_key, action,"
                    " result_ref, status, fingerprint, created_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (scope.role_id, request_key, "add_item",
                     json.dumps(result, ensure_ascii=False), "committed",
                     fingerprint, utc_now_iso()))
            return {"status": "committed", "result": result,
                    "dropped_storage_keys": dropped_keys}

    async def item_asset_ids(self, role_id: str, item_id: str) -> list[str]:
        db = self._require()
        cur = await db.execute(
            "SELECT asset_id FROM item_images WHERE role_id=? AND item_id=?"
            " ORDER BY ordinal, asset_id",
            (role_id, item_id))
        return [row["asset_id"] for row in await cur.fetchall()]

    async def get_asset(self, role_id: str, asset_id: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM image_assets WHERE role_id=? AND asset_id=? AND status='ready'",
            (role_id, asset_id))
        return await cur.fetchone()

    async def find_asset_by_hash(self, role_id: str,
                                 sha256_hex: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM image_assets WHERE role_id=? AND processed_sha256=?"
            " AND status='ready'",
            (role_id, sha256_hex))
        return await cur.fetchone()

    async def get_asset_by_key(self, storage_key: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM image_assets WHERE storage_key=?", (storage_key,))
        return await cur.fetchone()

    async def asset_reference_count(self, asset_id: str) -> int:
        """J2：资产被 item_images 实际引用的次数（清理判据）。"""
        db = self._require()
        cur = await db.execute(
            "SELECT COUNT(*) AS n FROM item_images WHERE asset_id=?", (asset_id,))
        return (await cur.fetchone())["n"]

    async def role_image_bytes(self, role_id: str) -> int:
        db = self._require()
        cur = await db.execute(
            "SELECT COALESCE(SUM(byte_size), 0) AS total FROM image_assets"
            " WHERE role_id=? AND status='ready'",
            (role_id,))
        return (await cur.fetchone())["total"]

    # ------------------------------------------------------------------
    # 原子删除（R6）：条目 + 引用 + 穿搭联动 + 孤儿资产认领同一事务
    # ------------------------------------------------------------------

    async def delete_item_verified(self, scope: TurnScope, persona_key: str | None,
                                   item_id: str, *, force: bool,
                                   is_owner: bool, is_maintainer: bool,
                                   pre_commit=None,
                                   ) -> dict:
        """单事务：复核 → 权限 → 正在穿判定 → 引用/条目/穿搭/孤儿行。

        返回 {deleted, outfit_revision, orphan_keys}；orphan_keys 是已从 DB
        认领（删除行）的资产存储键，文件删除在事务提交后执行。
        """
        db = self._require()
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            cur = await conn.execute(
                "SELECT * FROM wardrobe_items WHERE role_id=? AND item_id=?",
                (scope.role_id, item_id))
            row = await cur.fetchone()
            if row is None or not row["active"]:
                from .models import ItemNotFound

                raise ItemNotFound("服装不存在或已删除")
            wearing = [
                r["item_id"] for r in
                (await (await conn.execute(
                    "SELECT item_id FROM outfit_items WHERE role_id=?",
                    (scope.role_id,))).fetchall())]
            in_outfit = item_id in wearing
            if in_outfit and not (is_maintainer and force):
                from .models import PermissionDenied

                raise PermissionDenied(
                    "该服装正在当前穿搭中；普通删除被拒绝，管理者可明确强制删除")
            cur = await conn.execute(
                "SELECT asset_id FROM item_images WHERE role_id=? AND item_id=?",
                (scope.role_id, item_id))
            asset_ids = [r["asset_id"] for r in await cur.fetchall()]
            orphan_keys: list[str] = []
            for asset_id in asset_ids:
                await conn.execute(
                    "DELETE FROM item_images WHERE role_id=? AND item_id=?"
                    " AND asset_id=?",
                    (scope.role_id, item_id, asset_id))
                cur = await conn.execute(
                    "SELECT COUNT(*) AS n FROM item_images WHERE asset_id=?",
                    (asset_id,))
                if (await cur.fetchone())["n"] == 0:
                    cur = await conn.execute(
                        "SELECT storage_key FROM image_assets"
                        " WHERE role_id=? AND asset_id=?",
                        (scope.role_id, asset_id))
                    asset_row = await cur.fetchone()
                    if asset_row:
                        orphan_keys.append(asset_row["storage_key"])
                        await conn.execute(
                            "DELETE FROM image_assets WHERE role_id=? AND asset_id=?",
                            (scope.role_id, asset_id))
            await conn.execute(
                "UPDATE wardrobe_items SET active=0, revision=revision+1,"
                " updated_at=? WHERE role_id=? AND item_id=?",
                (utc_now_iso(), scope.role_id, item_id))
            outfit_revision = None
            if in_outfit:
                remaining = [i for i in wearing if i != item_id]
                cur = await conn.execute(
                    "SELECT revision, source_kind, manual_lock_until,"
                    " manual_lock_by_ref FROM outfit_state WHERE role_id=?",
                    (scope.role_id,))
                state = await cur.fetchone()
                new_revision = state["revision"] + 1
                await conn.execute(
                    "DELETE FROM outfit_items WHERE role_id=?", (scope.role_id,))
                for i in remaining:
                    await conn.execute(
                        "INSERT INTO outfit_items(role_id, item_id, slot,"
                        " outfit_revision) VALUES (?,?,?,?)",
                        (scope.role_id, i, "", new_revision))
                await conn.execute(
                    "UPDATE outfit_state SET revision=?, updated_at=? WHERE role_id=?",
                    (new_revision, utc_now_iso(), scope.role_id))
                outfit_revision = new_revision
            return {"deleted": item_id, "outfit_revision": outfit_revision,
                    "orphan_keys": orphan_keys}

    # ------------------------------------------------------------------
    # 穿搭（R6：原子替换，物品有效性在事务内复核）
    # ------------------------------------------------------------------

    async def replace_outfit(self, role_id: str, expected_revision: int, slots: list[tuple[str, str]],
                             source_kind: str, new_lock: str | None,
                             lock_by_ref: str | None) -> int:
        """乐观锁原子保存：expected_revision 不匹配时抛 VersionConflict。"""
        db = self._require()
        now = utc_now_iso()
        async with self.tx() as conn:
            cur = await conn.execute(
                "SELECT revision FROM outfit_state WHERE role_id=?", (role_id,))
            row = await cur.fetchone()
            if not row:
                raise LookupError("outfit_state 缺失（角色未初始化）")
            current = row["revision"]
            if current != expected_revision:
                raise VersionConflict(
                    "当前穿搭已被其他窗口修改", latest_revision=current)
            new_revision = current + 1
            for item_id, _slot in slots:
                cur = await conn.execute(
                    "SELECT active FROM wardrobe_items WHERE role_id=? AND item_id=?",
                    (role_id, item_id))
                item_row = await cur.fetchone()
                if item_row is None or not item_row["active"]:
                    from .models import ItemNotFound

                    raise ItemNotFound(f"服装 {item_id} 不存在或已删除")
            await conn.execute("DELETE FROM outfit_items WHERE role_id=?", (role_id,))
            for item_id, slot in slots:
                await conn.execute(
                    "INSERT INTO outfit_items(role_id, item_id, slot, outfit_revision)"
                    " VALUES (?,?,?,?)",
                    (role_id, item_id, slot, new_revision))
            await conn.execute(
                "UPDATE outfit_state SET revision=?, source_kind=?, manual_lock_until=?,"
                " manual_lock_by_ref=?, updated_at=? WHERE role_id=?",
                (new_revision, source_kind, new_lock, lock_by_ref, now, role_id))
        return new_revision

    async def replace_outfit_verified(self, scope: TurnScope, persona_key: str | None,
                                      item_ids: list[str], slot_map: dict[str, str],
                                      expected_revision: int, source_kind: str,
                                      new_lock: str | None, lock_by_ref: str | None,
                                      request_key: str | None, fingerprint: str | None,
                                      pre_commit=None, autonomy_check=None,
                                      ) -> dict:
        """穿搭原子提交：复核 + 锁检查 + 乐观版本 + 事务内组合重核 + 认领账本。

        G8：slot_map 是事务前的承诺，最终事务内重读全部 item 的
        category/layerable 并重新 validate_combination——类别/叠穿属性
        可能已在本请求执行期间被合法编辑。
        """
        db = self._require()
        async with self.tx(pre_commit) as conn:
            if request_key:
                cur = await conn.execute(
                    "SELECT action, result_ref, fingerprint FROM operation_ledger"
                    " WHERE role_id=? AND request_key=?",
                    (scope.role_id, request_key))
                prior = await cur.fetchone()
                if prior:
                    if fingerprint is not None and prior["fingerprint"] != fingerprint:
                        raise IdempotencyConflict(
                            "同一请求键已用于不同参数，已拒绝")
                    return {"status": "replayed",
                            "result": json.loads(prior["result_ref"] or "{}")}
            await self.verify_commit(conn, scope, persona_key)
            # M2：自主许可 gate（开关/窗口/turn 预算）原子核验与消耗
            if autonomy_check is not None:
                await autonomy_check(conn)
            cur = await conn.execute(
                "SELECT revision, manual_lock_until FROM outfit_state WHERE role_id=?",
                (scope.role_id,))
            state = await cur.fetchone()
            if not state:
                raise LookupError("outfit_state 缺失（角色未初始化）")
            from .outfits import lock_active, validate_combination

            if source_kind == "ai" and lock_active(state["manual_lock_until"]):
                from .models import ManualLockActive

                raise ManualLockActive(
                    "手工穿搭锁生效中，AI 选穿已拒绝；请等待锁过期或由管理者解锁",
                    until=state["manual_lock_until"])
            if state["revision"] != expected_revision:
                raise VersionConflict("当前穿搭已被其他窗口修改",
                                      latest_revision=state["revision"])
            new_revision = state["revision"] + 1
            entries: list[dict] = []
            for item_id in item_ids:
                cur = await conn.execute(
                    "SELECT active, category, layerable FROM wardrobe_items"
                    " WHERE role_id=? AND item_id=?",
                    (scope.role_id, item_id))
                item_row = await cur.fetchone()
                if item_row is None or not item_row["active"]:
                    from .models import ItemNotFound

                    raise ItemNotFound(f"服装 {item_id} 不存在或已删除")
                entries.append({"item_id": item_id,
                                "category": item_row["category"],
                                "layerable": bool(item_row["layerable"])})
            current_slots = validate_combination(entries)
            await conn.execute(
                "DELETE FROM outfit_items WHERE role_id=?", (scope.role_id,))
            for item_id in item_ids:
                await conn.execute(
                    "INSERT INTO outfit_items(role_id, item_id, slot, outfit_revision)"
                    " VALUES (?,?,?,?)",
                    (scope.role_id, item_id, current_slots[item_id], new_revision))
            await conn.execute(
                "UPDATE outfit_state SET revision=?, source_kind=?,"
                " manual_lock_until=?, manual_lock_by_ref=?, updated_at=?"
                " WHERE role_id=?",
                (new_revision, source_kind, new_lock, lock_by_ref,
                 utc_now_iso(), scope.role_id))
            result = {"revision": new_revision, "item_ids": list(item_ids),
                      "source_kind": source_kind}
            if request_key:
                await conn.execute(
                    "INSERT INTO operation_ledger(role_id, request_key, action,"
                    " result_ref, status, fingerprint, created_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (scope.role_id, request_key, "wear",
                     json.dumps(result, ensure_ascii=False), "committed",
                     fingerprint, utc_now_iso()))
            return {"status": "committed", "result": result}

    async def get_outfit_state(self, role_id: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM outfit_state WHERE role_id=?", (role_id,))
        return await cur.fetchone()

    async def get_outfit_items(self, role_id: str) -> list[aiosqlite.Row]:
        db = self._require()
        cur = await db.execute(
            "SELECT oi.item_id, oi.slot, oi.outfit_revision, wi.category, wi.active"
            " FROM outfit_items oi LEFT JOIN wardrobe_items wi"
            " ON wi.role_id = oi.role_id AND wi.item_id = oi.item_id"
            " WHERE oi.role_id=? ORDER BY oi.slot, oi.item_id",
            (role_id,))
        return list(await cur.fetchall())

    async def clear_manual_lock(self, role_id: str,
                                scope: TurnScope | None = None,
                                persona_key: str | None = None,
                                pre_commit=None) -> None:
        """G2：解锁在最终事务内复核维护权限。"""
        if scope is None:
            raise StaleContext("缺少本轮作用域，最终写事务拒绝执行")
        db = self._require()
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            await conn.execute(
                "UPDATE outfit_state SET manual_lock_until=NULL,"
                " manual_lock_by_ref=NULL, updated_at=? WHERE role_id=?",
                (utc_now_iso(), role_id))

    # ------------------------------------------------------------------
    # 自主开关（M2 工具使用；清空时也关闭）
    # ------------------------------------------------------------------

    async def set_autonomy(self, role_id: str, master: bool | None,
                           add: bool | None, wear: bool | None,
                           scope: TurnScope | None = None,
                           persona_key: str | None = None,
                           pre_commit=None) -> None:
        """Q-02：自主开关写入走最终权威——BEGIN 后 verify_commit（绑定
        revision/授权世代/窗口/逐人）+ 请求级宿主核验，失败整事务零副作用。"""
        db = self._require()
        async with self.tx(pre_commit) as conn:
            if scope is not None:
                await self.verify_commit(conn, scope, persona_key)
            if master is not None:
                await conn.execute(
                    "UPDATE autonomy_state SET master_enabled=?, revision=revision+1"
                    " WHERE role_id=?",
                    (1 if master else 0, role_id))
            if add is not None:
                await conn.execute(
                    "UPDATE autonomy_state SET add_enabled=?, revision=revision+1"
                    " WHERE role_id=?",
                    (1 if add else 0, role_id))
            if wear is not None:
                await conn.execute(
                    "UPDATE autonomy_state SET wear_enabled=?, revision=revision+1"
                    " WHERE role_id=?",
                    (1 if wear else 0, role_id))

    async def get_autonomy(self, role_id: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM autonomy_state WHERE role_id=?", (role_id,))
        return await cur.fetchone()

    # ------------------------------------------------------------------
    # 风格档案（M2；style_profiles 表在 v1 已建）
    # ------------------------------------------------------------------

    async def get_style_profile(self, role_id: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM style_profiles WHERE role_id=?", (role_id,))
        return await cur.fetchone()

    async def set_style_profile(self, role_id: str,
                                updates: dict[str, object],
                                scope: TurnScope | None = None,
                                persona_key: str | None = None,
                                pre_commit=None) -> dict:
        """Q-07：事务内对**最新档案**应用增量（仅 provided 字段）。

        并发的不同字段请求经 `_tx_lock` 串行各自读到最新值后应用——
        双成功不互相抹掉；同字段后写覆盖（revision 单调递增，规则一致）。
        Q-02：BEGIN 后 verify_commit + 请求级宿主核验同事务。返回真实
        提交后的完整档案与 revision。
        """
        db = self._require()
        now = utc_now_iso()
        async with self.tx(pre_commit) as conn:
            if scope is not None:
                await self.verify_commit(conn, scope, persona_key)
            cur = await conn.execute(
                "SELECT * FROM style_profiles WHERE role_id=?", (role_id,))
            row = await cur.fetchone()
            if row is None:
                base = {"preferred_tags_json": "[]", "avoid_tags_json": "[]",
                        "scene_preferences": "", "note": "", "revision": 0}
            else:
                base = dict(row)
            merged = {
                "preferred_tags_json": base["preferred_tags_json"],
                "avoid_tags_json": base["avoid_tags_json"],
                "scene_preferences": base["scene_preferences"],
                "note": base["note"],
            }
            if "喜欢" in updates:
                merged["preferred_tags_json"] = json.dumps(
                    updates["喜欢"], ensure_ascii=False)
            if "避开" in updates:
                merged["avoid_tags_json"] = json.dumps(
                    updates["避开"], ensure_ascii=False)
            if "场景" in updates:
                merged["scene_preferences"] = updates["场景"]
            if "说明" in updates:
                merged["note"] = updates["说明"]
            new_revision = base["revision"] + 1
            await conn.execute(
                "INSERT INTO style_profiles(role_id, preferred_tags_json,"
                " avoid_tags_json, scene_preferences, note, revision)"
                " VALUES (?,?,?,?,?,?)"
                " ON CONFLICT(role_id) DO UPDATE SET preferred_tags_json=?,"
                " avoid_tags_json=?, scene_preferences=?, note=?, revision=?",
                (role_id, merged["preferred_tags_json"],
                 merged["avoid_tags_json"], merged["scene_preferences"],
                 merged["note"], new_revision,
                 merged["preferred_tags_json"], merged["avoid_tags_json"],
                 merged["scene_preferences"], merged["note"], new_revision))
        return {"preferred_tags": json.loads(merged["preferred_tags_json"]),
                "avoid_tags": json.loads(merged["avoid_tags_json"]),
                "scene_preferences": merged["scene_preferences"],
                "note": merged["note"],
                "revision": new_revision, "updated_at": now}

    # ------------------------------------------------------------------
    # 自主用量（M2）：事务内原子 gate（任务#012 P-03 / #013 Q-03/Q-04）
    # ------------------------------------------------------------------

    @staticmethod
    def turn_identity(platform: str, umo: str, sender_ref: str,
                      message_id: str) -> str:
        """完整可信 turn 归属（窗口/actor 纳入——两窗口相同 message_id
        是不同 turn；与句柄/投影/收尾共用同一标识）。"""
        import hashlib

        raw = f"{platform}|{umo}|{sender_ref}|{message_id}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:32]

    def autonomy_gate(self, role_id: str, action: str, turn_key: str,
                      umo: str):
        """构造挂入写事务的自主许可 gate（BEGIN 后、业务写入前执行）。

        核验顺序：总开关 → 子开关 → 窗口 allow_ai_mutation → **持久
        turn 预算**（v4 autonomy_turn_usage 主键 INSERT，同 turn 同动作
        第二次即冲突拒绝——重启/异参/并发同样成立）→ UTC 日额度（仅
        add，按角色共享）→ 冷却（仅 add；**跨日持续**——日界只重置计
        数，不丢 last_add_at）。任一失败抛拒绝 → 调用方事务回滚，业务
        零副作用。日界/冷却一律服务器 UTC，不接受模型提供的时间。
        """
        async def gate(conn: aiosqlite.Connection) -> None:
            cur = await conn.execute(
                "SELECT master_enabled, add_enabled, wear_enabled"
                " FROM autonomy_state WHERE role_id=?", (role_id,))
            row = await cur.fetchone()
            if row is None:
                raise PermissionDenied("角色自主状态缺失")
            if not row["master_enabled"]:
                raise PermissionDenied("该角色自主行为已关闭")
            enabled = row["add_enabled"] if action == "add" \
                else row["wear_enabled"]
            if not enabled:
                raise PermissionDenied(
                    "该角色自主添衣已关闭" if action == "add"
                    else "该角色自主选穿已关闭")
            cur = await conn.execute(
                "SELECT allow_ai_mutation FROM session_grants"
                " WHERE role_id=? AND umo_ref=?", (role_id, umo))
            grant_row = await cur.fetchone()
            if grant_row is None or not grant_row["allow_ai_mutation"]:
                raise PermissionDenied("当前窗口未允许角色自主变更")
            # Q-04：持久 turn 预算——INSERT 与业务同事务，冲突即拒绝
            try:
                await conn.execute(
                    "INSERT INTO autonomy_turn_usage(role_id, turn_key,"
                    " action, created_at) VALUES (?,?,?,?)",
                    (role_id, turn_key, action, utc_now_iso()))
            except sqlite3.IntegrityError:
                raise PermissionDenied(
                    "本轮已提交一次自主添衣；如需修改请等待下一轮"
                    if action == "add" else "本轮已提交一次自主选穿")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            cur = await conn.execute(
                "SELECT * FROM autonomy_usage WHERE role_id=?", (role_id,))
            usage = await cur.fetchone()
            if usage is None:
                await conn.execute(
                    "INSERT INTO autonomy_usage(role_id, day_utc, add_count,"
                    " wear_count, last_add_message_id, last_wear_message_id,"
                    " last_add_at, last_wear_at, updated_at)"
                    " VALUES (?,?,0,0,'','',NULL,NULL,?)",
                    (role_id, today, utc_now_iso()))
                add_count = wear_count = 0
                last_add_at = None
            elif usage["day_utc"] != today:
                # Q-04：跨日只重置日计数——last_add_at 冷却跨日持续
                await conn.execute(
                    "UPDATE autonomy_usage SET day_utc=?, add_count=0,"
                    " wear_count=0, updated_at=? WHERE role_id=?",
                    (today, utc_now_iso(), role_id))
                add_count = wear_count = 0
                last_add_at = usage["last_add_at"]
            else:
                add_count = usage["add_count"]
                wear_count = usage["wear_count"]
                last_add_at = usage["last_add_at"]
            now = utc_now_iso()
            if action == "add":
                limit = self.config.autonomy_daily_add_limit
                if add_count >= limit:
                    raise ImageRejected(
                        f"今日自主添衣已达上限（{limit} 件），明日再试")
                cooldown = self.config.autonomy_cooldown_seconds
                if cooldown > 0 and last_add_at:
                    last = datetime.fromisoformat(
                        str(last_add_at).replace("Z", "+00:00"))
                    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
                    if elapsed < cooldown:
                        raise ImageRejected(
                            f"自主添衣冷却中，请约 {int(cooldown - elapsed) + 1}"
                            " 秒后再试")
                await conn.execute(
                    "UPDATE autonomy_usage SET add_count=?, last_add_at=?,"
                    " updated_at=? WHERE role_id=?",
                    (add_count + 1, now, now, role_id))
            else:
                await conn.execute(
                    "UPDATE autonomy_usage SET wear_count=?, updated_at=?"
                    " WHERE role_id=?", (wear_count + 1, now, role_id))
        return gate

    async def read_usage_snapshot(self, role_id: str) -> dict:
        """自主用量只读视图（service 展示用；非事务口径）。"""
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM autonomy_usage WHERE role_id=?", (role_id,))
        row = await cur.fetchone()
        return dict(row) if row is not None else {}

    async def ledger_get(self, role_id: str, request_key: str) -> aiosqlite.Row | None:
        db = self._require()
        cur = await db.execute(
            "SELECT * FROM operation_ledger WHERE role_id=? AND request_key=?",
            (role_id, request_key))
        return await cur.fetchone()

    async def ledger_put(self, role_id: str, request_key: str, action: str,
                         status: str, result_ref: str | None = None,
                         fingerprint: str | None = None) -> None:
        db = self._require()
        async with self.tx() as conn:
            await conn.execute(
                "INSERT INTO operation_ledger(role_id, request_key, action,"
                " result_ref, status, fingerprint, created_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (role_id, request_key, action, result_ref, status, fingerprint,
                 utc_now_iso()))

    async def record_settlement_hint(self, role_id: str, actor_ref: str,
                                     umo: str, request_key: str,
                                     fingerprint: str | None, action: str,
                                     credential: str, extra: dict) -> None:
        """I6：提交结果未知时登记公开核对提示（复用 audit_events，无新迁移）。

        detail 含凭据/原请求键/业务指纹/动作/窗口/脱敏发起者，全部只进
        本机 DB；核对时按凭据+窗口+发起者精确匹配后只读查询。
        """
        detail = {"cred": credential, "request_key": request_key,
                  "fingerprint": fingerprint, "action": action,
                  "umo": umo, "sender_ref": actor_ref}
        detail.update(extra or {})
        await self.audit(role_id, "settlement_unknown", actor_ref, detail,
                         "pending")

    async def verify_settlement(self, role_id: str, umo: str,
                                sender_ref: str, credential: str) -> dict:
        """I6/K2：公开原操作只读核对——聚合同一凭据的**全部**未决尝试。

        同操作键+业务指纹的自动重投/重放可有多次 unknown；每次尝试的
        文件/rid 各自生成并持久（record_settlement_hint 不覆盖旧清单）。
        核对收集全部匹配提示（同凭据+同窗口+同发起者），聚合：
        - 资源清单按 rid 去重合并（最新提示不遮早期所有者）；
        - 状态：任一尝试账本 committed → committed（携带原结果）；
          否则连接仍有未决事务/状态不可读 → pending；否则任一尝试的
          条目仍活跃（有条目无账本）→ pending；否则 not_committed。
        返回 {"state":…, "action":…, "result":…,
        "hint": {request_key/action/umo/sender_ref/resources/attempts}}。
        凭据无效或归属不符统一拒绝且不回显原请求细节；只读：不再执行
        add/wear、不占额度、不按文本指纹合并合法重复。
        """
        # L1：结果判定与尝试清单读取在同一 `_tx_lock` 写事务内完成——
        # 与一切写事务互斥后只读已结算视图：未提交 add 的 ledger.status
        # 不再被当作 durable 结果复制（反例：该 add 随后回滚，核对仍
        # 报 committed 及不存在 ID）。被未决事务阻挡时在此互斥等待
        # （等待者取消→幂等重试；所有者结算后立即可读，不无限 pending）。
        # 事务状态不可读取（连接故障）→ 如实 pending，不得下任何结论。
        db = self._require()
        state = await tx_state(db)
        if state is None:
            return {"state": "pending", "action": "verify"}
        async with self.tx() as conn:
            cur = await conn.execute(
                "SELECT detail_json FROM audit_events"
                " WHERE role_id=? AND action='settlement_unknown'"
                " ORDER BY audit_id DESC", (role_id,))
            attempts: list[dict] = []
            for row in await cur.fetchall():
                try:
                    detail = json.loads(row["detail_json"] or "{}")
                except (TypeError, ValueError):
                    continue
                if (isinstance(detail, dict)
                        and detail.get("cred") == credential
                        and detail.get("umo") == umo
                        and detail.get("sender_ref") == sender_ref):
                    attempts.append(detail)  # K2：不 break，全部尝试
            if not attempts:
                raise InvalidInput("核对凭据无效或不属于当前角色/窗口/发起者")
            match = attempts[0]  # 最新尝试提供操作级字段
            request_key = str(match.get("request_key") or "")
            action = str(match.get("action") or "")
            # K2：聚合全部尝试的资源清单（按 rid 去重，保留旧尝试入口）
            merged: list[dict] = []
            seen_rids: set[str] = set()
            for detail in reversed(attempts):  # 时间正序，旧尝试在前
                for res in detail.get("resources") or []:
                    rid = res.get("rid") or ""
                    if rid and rid in seen_rids:
                        continue
                    if rid:
                        seen_rids.add(rid)
                    merged.append(res)
            hint = {"request_key": request_key, "action": action,
                    "umo": umo, "sender_ref": sender_ref,
                    "resources": merged, "attempts": len(attempts)}
            # 已结算视图内的账本判定：此处读到的 committed 一定已提交
            ledger = None
            if request_key:
                lcur = await conn.execute(
                    "SELECT status, result_ref FROM operation_ledger"
                    " WHERE role_id=? AND request_key=?", (role_id, request_key))
                ledger = await lcur.fetchone()
            if ledger is not None and ledger["status"] == "committed":
                try:
                    result = json.loads(ledger["result_ref"] or "{}")
                except (TypeError, ValueError):
                    result = {}
                return {"state": "committed", "action": action,
                        "result": result, "hint": hint}
            # 已结算视图内无账本行：检查任一尝试条目是否活跃
            if action == "add_item":
                for detail in attempts:
                    item_id = str(detail.get("item_id") or "")
                    if item_id:
                        icur = await conn.execute(
                            "SELECT active FROM wardrobe_items"
                            " WHERE role_id=? AND item_id=?",
                            (role_id, item_id))
                        irow = await icur.fetchone()
                        if irow is not None and irow["active"]:
                            # 有条目无账本行：状态异常，保守待核对
                            return {"state": "pending", "action": action,
                                    "hint": hint}
            return {"state": "not_committed", "action": action, "hint": hint}

    async def audit(self, role_id: str | None, action: str, actor_ref: str | None,
                    detail: dict, result: str) -> None:
        db = self._require()
        async with self.tx() as conn:
            await conn.execute(
                "INSERT INTO audit_events(role_id, action, actor_ref, detail_json,"
                " result, created_at) VALUES (?,?,?,?,?,?)",
                (role_id, action, actor_ref,
                 json.dumps(detail, ensure_ascii=False), result, utc_now_iso()))

    # ------------------------------------------------------------------
    # 清空 / 孤儿（R6：孤儿认领在事务内，杜绝旧快照误删）
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate_hint_resources(rows) -> list[dict]:
        """M-01：按 K2 口径聚合提示资源（rid 去重，旧尝试在前）。"""
        attempts: list[dict] = []
        for row in rows:
            try:
                detail = json.loads(row["detail_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if isinstance(detail, dict):
                attempts.append(detail)
        merged: list[dict] = []
        seen_rids: set[str] = set()
        for detail in reversed(attempts):
            for res in detail.get("resources") or []:
                rid = res.get("rid") or ""
                if rid and rid in seen_rids:
                    continue
                if rid:
                    seen_rids.add(rid)
                merged.append(res)
        return merged

    async def clear_role(self, role_id: str,
                         scope: TurnScope | None = None,
                         persona_key: str | None = None,
                         pre_commit=None) -> dict:
        """G2/M-01：清空在同一事务内判权后删除全部角色数据。

        返回 {"keys": [storage_key…], "retired_resources": […]}：keys 是
        已登记资产键（提交后由调用方 unlink）；retired_resources 是删除
        映射前捕获的全部未决尝试资源（K2 聚合口径），并以持久 tombstone
        （audit_events action='settlement_retired'，与 clear 同事务原子）
        留痕——角色映射删除后由内部退休所有者结算，不依赖已失效的公共
        请求，也不借公共核对越权。clear 回滚时 tombstone 一并回滚，旧
        衣柜与合法核对入口保持。
        """
        if scope is None:
            raise StaleContext("缺少本轮作用域，最终写事务拒绝执行")
        db = self._require()
        async with self.tx(pre_commit) as conn:
            await self.verify_commit(conn, scope, persona_key)
            cur = await conn.execute(
                "SELECT storage_key FROM image_assets WHERE role_id=?", (role_id,))
            keys = [r["storage_key"] for r in await cur.fetchall()]
            # M-01/N-02：删除映射前捕获 clear 接管的全部可信资源——
            # 登记资产键（普通 add 无 hint 也覆盖）+ 全部未决尝试资源
            # （K2 聚合），按键合并（同键保留未决 rid/temp）
            hcur = await conn.execute(
                "SELECT detail_json FROM audit_events"
                " WHERE role_id=? AND action='settlement_unknown'"
                " ORDER BY audit_id DESC", (role_id,))
            hint_resources = self._aggregate_hint_resources(
                await hcur.fetchall())
            merged: dict[str, dict] = {}
            order: list[str] = []
            for k in keys:
                merged[k] = {"rid": "", "key": k, "temp": ""}
                order.append(k)
            for res in hint_resources:
                k = res.get("key") or ""
                rid = res.get("rid") or ""
                temp = res.get("temp") or ""
                if k and k in merged:
                    if rid and not merged[k]["rid"]:
                        merged[k]["rid"] = rid
                    if temp and not merged[k]["temp"]:
                        merged[k]["temp"] = temp
                    continue
                key_id = k or (f"rid:{rid}" if rid else
                               (f"tmp:{temp}" if temp else ""))
                if key_id in merged:
                    continue
                merged[key_id] = {"rid": rid, "key": k, "temp": temp}
                order.append(key_id)
            retired = [merged[k] for k in order]
            # 持久 tombstone：与 clear 提交原子，供 unknown 调解/重启留痕
            await conn.execute(
                "INSERT INTO audit_events(role_id, action, actor_ref,"
                " detail_json, result, created_at) VALUES (?,"
                " 'settlement_retired', NULL, ?, 'pending', ?)",
                (role_id,
                 json.dumps({"resources": retired}, ensure_ascii=False),
                 utc_now_iso()))
            for table in ("outfit_items", "item_images", "image_assets",
                          "wardrobe_items", "outfit_state", "operation_ledger",
                          "autonomy_state", "style_profiles", "session_grants",
                          "sender_grants", "persona_bindings"):
                await conn.execute(
                    f"DELETE FROM {table} WHERE role_id=?", (role_id,))
            await conn.execute("DELETE FROM roles WHERE role_id=?", (role_id,))
            await self._record_auth_event(conn, role_id, "clear")
        return {"keys": keys, "retired_resources": retired}

    async def read_retired_tombstone(self, role_id: str) -> list[dict] | None:
        """M-01：读取 clear 的持久 tombstone（unknown 调解/重启恢复用）。

        返回 None 表示角色仍存在（clear 已回滚）或无 tombstone。
        """
        db = self._require()
        cur = await db.execute(
            "SELECT detail_json FROM audit_events"
            " WHERE role_id=? AND action='settlement_retired'"
            " ORDER BY audit_id DESC LIMIT 1", (role_id,))
        row = await cur.fetchone()
        if row is None:
            return None
        try:
            detail = json.loads(row["detail_json"] or "{}")
        except (TypeError, ValueError):
            return []
        return detail.get("resources") or []

    def audit_raw(self, role_id: str | None, action: str, detail: dict,
                  result: str) -> None:
        """M-01/N-01：连接隔离（quarantine）后的 best-effort 审计留痕。

        用独立 sqlite3 连接直接追加 audit_events（append-only 日志），
        仅供状态不可读等无法经主连接写入的场景记录责任边界；失败静默。
        """
        try:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO audit_events(role_id, action, actor_ref,"
                    " detail_json, result, created_at) VALUES (?,?,NULL,?,?,?)",
                    (role_id, action,
                     json.dumps(detail, ensure_ascii=False), result,
                     utc_now_iso()))
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    async def claim_orphan_assets(self) -> list[aiosqlite.Row]:
        """事务内认领：删除无引用资产行并返回其 (role_id, asset_id, storage_key)。

        认领与删除同一事务；并发的 insert_item_verified 被事务锁挡住，
        不会出现“用旧孤儿快照删掉并发新增引用”的竞态。
        """
        db = self._require()
        async with self.tx() as conn:
            cur = await conn.execute(
                "SELECT a.role_id, a.asset_id, a.storage_key FROM image_assets a"
                " LEFT JOIN item_images i ON i.asset_id = a.asset_id"
                " WHERE i.asset_id IS NULL")
            orphans = list(await cur.fetchall())
            for row in orphans:
                await conn.execute(
                    "DELETE FROM image_assets WHERE role_id=? AND asset_id=?",
                    (row["role_id"], row["asset_id"]))
            return orphans
