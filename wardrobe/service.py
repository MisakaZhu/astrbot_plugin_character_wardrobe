"""统一领域服务：衣服管理、图片协调、穿搭、授权与清空。

整改要点：
- R3：所有写入路径经 repository 的 *_verified 原子操作，在最终事务内复核
  绑定/世代/授权/逐人权限；scope=None 表示全新请求（按当前状态构造快照），
  main.py 命令层必须传入真实 TurnScope。
- H1：宿主 persona/管理员重验回调是**请求级**参数（host_verify），不再
  使用可被并发请求互相覆盖/清除的共享字段；回调在最终事务准入后、
  敏感操作生效前执行（repository._Tx.pre_commit）。
- H4：COMMIT 结果无法确认时向调用方传播 TxOutcomeUnknown——调用方
  不得删除可能已被 DB 引用的文件，效果以幂等重放/受控恢复结算。
- H5：文件补偿整体后台任务化——多次取消不中断清理，停用等待其完成。
- H6：stop 建立停止准入 → 在途请求结算 → 后台补偿结算 → 工人结算 →
  DB 关闭的完整顺序；重复 stop 幂等；取消 stop 后再次 stop 继续。
- R5：添加流程分两阶段——先在事务外完成全部图片处理（decode→重编码→临时
  落盘→rename），再单事务写入条目/资产/引用/账本；任一步骤失败（含取消）
  只清理本次新建文件，既有去重资产不受影响。
- H7：每请求图片数量与 processed 字节总额独立核算（与全服务 pending
  预算、每角色已提交配额分开）；pending 预留按请求所有权记账。
- R6/R7：配额、幂等认领与业务效果同事务；指纹由 canonical_fingerprint 生成。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from pathlib import Path


from .config import (
    DESC_MAX_LEN,
    LIST_PAGE_SIZE,
    NAME_MAX_LEN,
    WardrobeConfig,
)
from .host_adapter import build_scope
from .images import ImageAssetService
from .models import (
    ALL_CATEGORIES,
    NotInitialized,
    RoleUnbound,
    settlement_credential,
    InvalidInput,
    ItemNotFound,
    IdempotencyConflict,
    ImageRejected,
    PermissionDenied,
    PermissionLevel,
    QuotaExceeded,
    ServiceStopping,
    StaleContext,
    TxOutcomeUnknown,
    TurnScope,
    WardrobeItem,
    canonical_fingerprint,
    clean_tags,
    clean_text,
    utc_now_iso,
)
from .outfits import OutfitService, validate_combination
from .permissions import GrantSnapshot, PermissionService
from .repository import Repository, tx_state
from .roles import RoleResolver


class WardrobeService:
    """启动/停止生命周期 + 全部领域操作。"""

    def __init__(self, data_root: Path, config: WardrobeConfig,
                 trusted_media_roots: list[Path] | None = None) -> None:
        self.config = config
        self.repo = Repository(data_root / "wardrobe.db", config)
        self.roles = RoleResolver(self.repo)
        self.permissions = PermissionService(self.repo)
        self.outfits = OutfitService(self.repo, config)
        from .styles import StyleService

        self.styles = StyleService(self.repo, config)
        self.images = ImageAssetService(data_root, self.repo, config,
                                        trusted_media_roots)
        # H6：停止准入与在途请求/后台补偿结算
        self._closing = False
        self._inflight: set[asyncio.Task] = set()
        self._background: set[asyncio.Task] = set()
        # O-01：有限重试耗尽后的持续退休责任（tombstone 在 DB，为事实源）
        self._retirement_backlog: set[str] = set()
        self._retirement_caretaker_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._closing = False
        await self.repo.start()
        self.images.assets_root.mkdir(parents=True, exist_ok=True)
        self.images.tmp_root.mkdir(parents=True, exist_ok=True)
        await self.repo.audit(None, "service_start", None, {}, "ok")

    async def stop(self) -> None:
        """H6/I4：停止准入 → 在途请求 → 后台补偿 → 工人/结算 → DB 关闭。

        等待一律 asyncio.wait + shield 语义：stop 自身被取消不会把取消
        传播给在途请求或任何结算所有者；重复 stop 幂等（从中断处继续）。
        DB 关闭必须在全部结算所有者真实完成之后（repo.settle_all）。
        """
        self._closing = True
        try:
            while self._inflight:
                await self._wait_all(self._inflight)
            while self._background:
                await self._wait_all(self._background)
            await self.images.close()
            await self.repo.settle_all()
            if self.repo.is_open:
                try:
                    await self.repo.audit(None, "service_stop", None, {},
                                          "ok")
                except Exception:
                    pass  # 故障连接上的收尾审计不阻断关闭
                await self.repo.stop()
        except asyncio.CancelledError:
            raise
        except Exception:
            # 尽力关闭：即便结算异常也释放 DB 连接，避免悬挂
            if self.repo.is_open:
                try:
                    await self.repo.stop()
                except Exception:
                    pass
            raise

    @staticmethod
    async def _wait_all(tasks: set) -> None:
        """I4：等待一组任务真实完成；等待被取消不取消任何任务。"""
        waiting = list(tasks)
        while waiting:
            _done, pending = await asyncio.wait(waiting)
            waiting = list(pending)

    # ------------------------------------------------------------------
    # H6：停止准入 + 在途请求登记
    # ------------------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _request(self):
        if self._closing:
            raise ServiceStopping("服务正在停用，新请求已拒绝")
        task = asyncio.current_task()
        self._inflight.add(task)
        try:
            yield
        finally:
            self._inflight.discard(task)

    def _bg(self, coro) -> asyncio.Task:
        """登记后台补偿/结算任务（H5）：stop 会等待其完成。"""
        task = asyncio.ensure_future(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    # ------------------------------------------------------------------
    # 作用域构造：scope=None 时按当前状态构造全新请求快照
    # ------------------------------------------------------------------

    async def _resolve_and_check(
        self, persona_key: str | None, scope: TurnScope | None, action: str,
        is_host_admin: bool, umo: str, sender_ref: str, minimum_level: str,
    ) -> tuple[str, TurnScope, GrantSnapshot]:
        role = await self.roles.resolve(persona_key,
                                        scope.binding_revision if scope else None,
                                        scope.role_id if scope else None)
        snapshot = await self.permissions.snapshot(
            role.role_id, umo, sender_ref, is_host_admin,
            scope.grant_revision if scope else None,
            scope.auth_generation if scope else None)
        self.permissions.check_action(snapshot, action)
        effective = TurnScope(
            role_id=role.role_id,
            binding_revision=scope.binding_revision if scope else role.binding_revision,
            grant_revision=scope.grant_revision if scope else snapshot.revision,
            turn_token=scope.turn_token if scope else f"svc:{uuid.uuid4().hex[:12]}",
            umo=umo,
            sender_ref=sender_ref,
            auth_generation=(scope.auth_generation if scope
                             else snapshot.auth_generation),
            is_host_admin=is_host_admin,
            minimum_level=minimum_level,
        )
        return role.role_id, effective, snapshot

    # ------------------------------------------------------------------
    # H1：请求级宿主核验 → 最终事务准入后执行（repository._Tx.pre_commit）
    # ------------------------------------------------------------------

    @staticmethod
    def _host_pre_commit(host_verify, persona_key: str | None,
                         is_host_admin: bool):
        """把请求级宿主核验回调包装为事务准入钩子。

        host_verify 是 main 层注入的重验函数（重走宿主 persona 解析链），
        返回 (当前 persona_key, 当前管理员态)；无法获取、persona 或管理员
        状态变化一律 StaleContext fail closed。host_verify=None（无宿主
        事件可查的内部直调）时不产生钩子，最终事务仍执行 DB 世代复核。
        """
        if host_verify is None:
            return None

        async def pre_commit() -> None:
            try:
                current_key, current_admin = await host_verify()
            except Exception as exc:
                raise StaleContext(
                    "无法重新获取当前宿主状态；请重新发起请求") from exc
            if current_key is None or current_key != persona_key:
                raise StaleContext(
                    "宿主最终人格在请求执行期间发生变化，旧请求作废")
            if bool(current_admin) != bool(is_host_admin):
                raise StaleContext(
                    "宿主管理员状态在请求执行期间发生变化，旧请求作废")

        return pre_commit

    # ------------------------------------------------------------------
    # 初始化 / 授权 / 清空
    # ------------------------------------------------------------------

    async def initialize(self, persona_key: str | None, display_name: str,
                         actor_ref: str, umo: str, host_verify=None,
                         is_host_admin: bool = False) -> dict:
        """初始化角色（I1）：管理员专用，最终写事务准入后重验宿主权威。

        host_verify 为 main 注入的请求级回调（重走宿主 persona 解析链），
        在 BEGIN 之后、建角色/绑定/授权/autonomy 任一写入之前执行；
        persona 变化、管理员降权或状态不可获取一律 StaleContext，
        零角色/绑定/授权/autonomy 副作用。不引入 TurnScope（初始化前
        角色不存在），也不回退共享回调。
        """
        async with self._request():
            display = clean_text(display_name, field_name="角色名",
                                 max_len=NAME_MAX_LEN, allow_symbols=False)
            pre_commit = self._host_pre_commit(host_verify, persona_key,
                                               is_host_admin)
            role = await self.roles.initialize(persona_key, display, actor_ref,
                                               umo, pre_commit=pre_commit)
            await self.repo.audit(role.role_id, "initialize", actor_ref,
                                  {"umo_present": bool(umo)}, "ok")
            return {"role_id": role.role_id,
                    "binding_revision": role.binding_revision,
                    "display_name": display}

    async def grant_window(self, persona_key: str | None, scope: TurnScope | None,
                           actor_ref: str, umo: str, is_host_admin: bool,
                           target_umo: str, allow_ai: bool = True,
                           host_verify=None) -> dict:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "grant", is_host_admin, umo, actor_ref,
                "maintainer")
            if not target_umo or ":" not in target_umo:
                raise InvalidInput("目标窗口 UMO 不合法（需要 platform:type:id 形态）")
            rev = await self.repo.set_grant(
                role_id, target_umo, allow_ai, eff, persona_key,
                self._host_pre_commit(host_verify, persona_key,
                                      is_host_admin))
            await self.repo.audit(role_id, "grant_window", actor_ref,
                                  {"level": "reader(window)"}, "ok")
            return {"grant_revision": rev}

    async def grant_sender(self, persona_key: str | None, scope: TurnScope | None,
                           actor_ref: str, umo: str, is_host_admin: bool,
                           target_sender_ref: str, level: str | None,
                           host_verify=None) -> dict:
        """设置/移除逐人级别。level ∈ {contributor, maintainer, None(降回 reader)}。"""
        if level not in ("contributor", "maintainer", None):
            raise PermissionDenied("逐人级别只能是 贡献/管理（或撤除为读取）")
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "grant", is_host_admin, umo, actor_ref,
                "maintainer")
            await self.repo.set_sender_grant(
                role_id, umo, target_sender_ref, level, eff, persona_key,
                self._host_pre_commit(host_verify, persona_key,
                                      is_host_admin))
            await self.repo.audit(role_id, "grant_sender", actor_ref,
                                  {"level": level or "reader"}, "ok")
            return {"ok": True}

    async def revoke(self, persona_key: str | None, scope: TurnScope | None,
                     actor_ref: str, umo: str, is_host_admin: bool,
                     target_umo: str, host_verify=None) -> bool:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "revoke", is_host_admin, umo, actor_ref,
                "maintainer")
            ok = await self.repo.revoke_grant(
                role_id, target_umo, eff, persona_key,
                self._host_pre_commit(host_verify, persona_key,
                                      is_host_admin))
            await self.repo.audit(role_id, "revoke", actor_ref, {},
                                  "ok" if ok else "noop")
            return ok

    async def clear_role(self, persona_key: str | None, scope: TurnScope | None,
                         actor_ref: str, umo: str, is_host_admin: bool,
                         confirm_token: str, host_verify=None) -> dict:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "clear", is_host_admin, umo, actor_ref,
                "maintainer")
            if clean_text(confirm_token, field_name="确认参数", max_len=16,
                          allow_empty=False, allow_symbols=False) != "confirm":
                raise PermissionDenied("清空需要显式 confirm 参数，已拒绝")
            try:
                result = await self.repo.clear_role(
                    role_id, eff, persona_key,
                    self._host_pre_commit(host_verify, persona_key,
                                          is_host_admin))
            except TxOutcomeUnknown:
                # M-01：clear COMMIT 已完成但结果未知的真实 unknown 路径
                # ——不误删、不误报；owned 调解者读已结算视图后按
                # tombstone 结算或确认回滚，持有后续结算责任
                self._bg(self._reconcile_clear_unknown(role_id))
                raise
            keys = result["keys"]
            retired_resources = result.get("retired_resources") or []
            # N-01：无空隙交接——提交后任何 await 之前先创建 owned 结束
            # 所有者；附加审计等待被取消、审计异常、同步 unlink 故障均
            # 不再影响任务创建（tombstone 已随 clear 原子提交）
            self._bg(self._finalize_cleared(
                role_id, actor_ref, keys, retired_resources))
            removed = 0
            try:
                for key in keys:
                    try:
                        path = self.images.storage_path(key)
                        if path.exists():
                            path.unlink()
                            removed += 1
                    except Exception:
                        continue  # 登记键在 tombstone 中，所有者幂等重试
                # 附加审计 best-effort：失败不掩盖已提交结果、不跳过退休
                await self.repo.audit(role_id, "clear", actor_ref,
                                      {"assets_removed": removed,
                                       "retired": len(retired_resources)},
                                      "ok")
            except asyncio.CancelledError:
                raise  # 等待者取消如实传播；所有者持续结算
            except Exception:
                pass
            return {"cleared": True, "assets_removed": removed,
                    "retired": len(retired_resources)}

    async def _finalize_cleared(self, role_id: str, actor_ref: str,
                                keys: list[str],
                                resources: list[dict]) -> None:
        """N-01：clear 提交后的完整 owned 结束阶段（无空隙交接）。

        创建于 repo.clear_role 返回之后、任何 await 之前：同步 unlink/
        附加审计的取消或异常不影响本所有者。K1 协议快速退休 3 次；
        leftover 非空时审计 partial 并创建内部调度者继续有界重试。
        """
        leftover = await self._retire_cleared_resources(role_id, resources)
        if leftover:
            self._bg(self._schedule_retirement_retries(
                role_id, resources, leftover))

    async def _retire_cleared_resources(self, role_id: str,
                                        retired_resources: list[dict],
                                        ) -> list[dict]:
        """M-01：clear 后的内部退休所有者（单轮 3 次快速尝试）。

        复用 K1 认领协议（不传 scope）：按真实 DB 引用/文件消失结算
        retired 资源，确认消失才按各自 rid 幂等释放；leftover 受控保留
        （O-02：未完成资源描述，含空 rid 的普通登记键）。多次取消不中
        断；stop 等待其真实完成。
        """
        if not retired_resources:
            return []
        leftover: list[str] = []
        for _attempt in range(3):  # 有界快速重试（清理故障受控保留）
            leftover = await self._settle_request_resources(
                role_id, retired_resources)
            if not leftover:
                break
            await asyncio.sleep(0.05)
        try:
            await self.repo.audit(
                role_id, "settlement_retired_settled", None,
                {"resources": len(retired_resources), "leftover": leftover},
                "ok" if not leftover else "partial")
        except Exception:
            pass  # 退休本身已完成，附加审计失败不回滚结算
        return leftover

    async def _schedule_retirement_retries(self, role_id: str,
                                           resources: list[dict],
                                           leftover: list[dict]) -> None:
        """N-01/O-01：快速重试耗尽后的内部调度（有界速率 + 可达接管）。

        0.25s×8 + 0.5s×6 幂等重试：文件系统恢复后自动接管，全部结算→
        审计 ok；耗尽→把 role_id 注册进 `_retirement_backlog` 并启动
        caretaker 消费者（同实例可达受控责任，tombstone 为事实源），
        再写 partial/pending 审计。不忙循环、不占准入；stop 等待本任务
        真实结束后才关 DB。
        """
        pending = list(leftover)
        for attempt in range(14):
            await asyncio.sleep(0.25 if attempt < 8 else 0.5)
            try:
                pending = await self._settle_request_resources(
                    role_id, resources)
            except Exception:
                continue  # 连接故障：下一 tick 重试
            if not pending:
                try:
                    await self.repo.audit(
                        role_id, "settlement_retired_settled", None,
                        {"resources": len(resources), "via": "scheduled"},
                        "ok")
                except Exception:
                    pass
                return
        # O-01：耗尽不停责——持久清单（tombstone）交由同实例可达入口接管
        self._retirement_backlog.add(role_id)
        self._ensure_retirement_caretaker()
        try:
            await self.repo.audit(
                role_id, "settlement_reconcile_pending", None,
                {"leftover": pending}, "pending")
        except Exception:
            pass  # tombstone 持久留痕；caretaker/重启恢复为最终边界

    _RETIREMENT_CARETAKER_INTERVAL_S = 1.0

    def _ensure_retirement_caretaker(self) -> None:
        """O-01：启动（或复职）退休责任消费者——单例、幂等的同实例入口。"""
        if self._closing:
            return  # 停用中不再接管：tombstone/pending 审计为责任边界
        task = self._retirement_caretaker_task
        if task is not None and not task.done():
            return
        task = self._bg(self._retirement_caretaker())
        self._retirement_caretaker_task = task

        def _revive(done: asyncio.Task) -> None:
            # 消费者意外终止（如被取消）而责任未清偿时自动复职；
            # 正常退出（清单清空）或停用中不复职
            if not self._closing and self._retirement_backlog:
                self._ensure_retirement_caretaker()

        task.add_done_callback(_revive)

    async def _retirement_caretaker(self) -> None:
        """O-01：有限重试耗尽后的持续责任消费者。

        仅当存在未完成退休清单时运行（清单清偿即退出，不留常驻任务）；
        有界间隔幂等重试 tombstone 资源（K1 协议，锁顺序不变）：文件
        系统恢复后同实例自动接管，确认消失后按各自 rid 幂等释放。tomb-
        stone 消失（真回滚）即解除责任。stop（_closing）后不再开始新
        尝试并退出——等待者可见、DB 最后关闭；tombstone 与 pending 审
        计持久留痕，重启后 recover_untracked_files 为最终边界。
        """
        while not self._closing:
            if not self._retirement_backlog:
                return  # 责任已清偿：消费者退出
            await asyncio.sleep(self._RETIREMENT_CARETAKER_INTERVAL_S)
            if self._closing or not self._retirement_backlog:
                return
            for role_id in sorted(self._retirement_backlog):
                try:
                    tombstone = await self.repo.read_retired_tombstone(role_id)
                except Exception:
                    continue  # 下一 tick 重试
                if not tombstone:
                    self._retirement_backlog.discard(role_id)
                    continue
                try:
                    leftover = await self._settle_request_resources(
                        role_id, tombstone)
                except Exception:
                    continue  # 下一 tick 重试
                if not leftover:
                    self._retirement_backlog.discard(role_id)
                    try:
                        await self.repo.audit(
                            role_id, "settlement_retired_settled", None,
                            {"resources": len(tombstone),
                             "via": "caretaker"}, "ok")
                    except Exception:
                        pass  # 结算已完成，附加审计失败不回滚

    async def _reconcile_clear_unknown(self, role_id: str) -> None:
        """M-01：clear 自身 TxOutcomeUnknown 的 owned 调解者。

        有界循环读已结算视图：角色仍在（clear 真实回滚）→ 不退休，旧
        衣柜与核对入口保持；角色已删 → 按持久 tombstone 结算 retired
        资源；连接状态不可读 → 保持持有并留审计（重启后
        recover_untracked_files 清孤儿、tombstone 留痕）。
        """
        for _attempt in range(5):
            try:
                db = self.repo._require()
            except RuntimeError:
                # 连接已隔离（fail closed）：立即落审计责任并退出
                break
            try:
                state = await tx_state(db)
                if state is None:
                    await asyncio.sleep(0.05)
                    continue
                async with self.repo.tx() as conn:
                    cur = await conn.execute(
                        "SELECT COUNT(*) AS n FROM roles WHERE role_id=?",
                        (role_id,))
                    role_gone = (await cur.fetchone())["n"] == 0
                if not role_gone:
                    return  # clear 实际回滚：旧衣柜保持，无需退休
                tombstone = await self.repo.read_retired_tombstone(role_id)
                if tombstone:
                    leftover = await self._retire_cleared_resources(
                        role_id, tombstone)
                    if leftover:
                        # O-01：unknown 调解后的未完成清单移交同一调度者
                        #（耗尽后注册 backlog/caretaker，known/unknown
                        # 共用等效责任链），不由调解者静默丢弃
                        self._bg(self._schedule_retirement_retries(
                            role_id, tombstone, leftover))
                return
            except Exception:
                await asyncio.sleep(0.05)
                continue
        # 连接可能已隔离：审计责任经独立连接留痕（N-01 可达责任）
        self.repo.audit_raw(role_id, "settlement_reconcile_pending", {},
                            "pending")

    # ------------------------------------------------------------------
    # 衣服 CRUD
    # ------------------------------------------------------------------

    async def add_item(self, persona_key: str | None, scope: TurnScope | None,
                       actor_ref: str, umo: str, is_host_admin: bool, name: str,
                       description: str, category: str, tags, layerable: bool,
                       source_kind: str, handles: list | None = None,
                       request_key: str | None = None,
                       host_verify=None, autonomy_check=None,
                       fingerprint: str | None = None) -> dict:
        """添加服装（G3/I2/I4/I5：图片事务外处理 → 单事务写入全部引用）。

        prepare 不在事务外判去重——旧图可能在处理期间被合法删除；全部
        资产候选进入最终事务按 digest 重新判定。
        I4：rename 经 images.stage_move 提交为 owned 结算任务；补偿在真实
        rename 结算后对最终落点/临时路径做真实存在采样清理，晚完成的
        写入逃不出清理清单。
        I5：物理写盘前按实际输出原子预留/记账拒绝；每请求 processed 总额
        与图数在对应写盘前约束；预留持续覆盖到 committed 计入持久配额或
        补偿真实清理完成——请求 finally 不再提前归还。
        I2：TxOutcomeUnknown 时保留文件与预留并登记公开核对凭据（I6）；
        确定未提交才进入文件补偿。
        H1：host_verify 请求级宿主核验在最终事务准入后执行。
        M2：autonomy_check（source_kind="ai" 时传 repository.autonomy_gate）
        在同一写事务 BEGIN 后核验开关/turn 预算/日额度/冷却并原子消耗。
        R-02：fingerprint 由调用方显式传入时原样用于账本与核对凭据
        （AI 路径的完整规范工具参数单一来源）；不传时维持既有手工
        命令语义（服务端按规范业务字段+图片源重算）。
        """
        async with self._request():
            return await self._add_item_inner(
                persona_key, scope, actor_ref, umo, is_host_admin, name,
                description, category, tags, layerable, source_kind, handles,
                request_key, host_verify, autonomy_check, fingerprint)

    async def _add_item_inner(self, persona_key, scope, actor_ref, umo,
                              is_host_admin, name, description, category,
                              tags, layerable, source_kind, handles,
                              request_key, host_verify,
                              autonomy_check=None,
                              fingerprint: str | None = None) -> dict:
        role_id, eff, snapshot = await self._resolve_and_check(
            persona_key, scope, "add", is_host_admin, umo, actor_ref,
            "contributor")
        if source_kind == "ai" and not snapshot.allow_ai_mutation:
            raise PermissionDenied("当前窗口未允许 AI 修改衣柜")
        name_c = clean_text(name, field_name="名称", max_len=NAME_MAX_LEN,
                            allow_symbols=False)
        desc_c = clean_text(description, field_name="描述", max_len=DESC_MAX_LEN,
                            allow_empty=True)
        cat_c = clean_text(category, field_name="类别", max_len=12,
                           allow_symbols=False)
        if cat_c not in ALL_CATEGORIES:
            raise InvalidInput(f"类别必须是 {'、'.join(ALL_CATEGORIES)}")
        tags_c = clean_tags(tags)
        # I5：每请求图片数量硬上限（先于任何物理写盘）
        if handles and len(handles) > self.config.max_images_per_request:
            raise InvalidInput(
                f"单次至多附带 {self.config.max_images_per_request} 张图片")
        digest_sources = [getattr(h, "payload", "") for h in (handles or [])]
        if fingerprint is None:
            fingerprint = canonical_fingerprint(
                "add_item", name=name_c, description=desc_c, category=cat_c,
                tags=tags_c, layerable=bool(layerable), source_kind=source_kind,
                image_sources=digest_sources) if request_key else None

        # ---- 阶段 A：事务外完成全部图片处理（R6：不持事务锁）----
        prepared: list[dict] = []          # 全部资产候选（含 temp 文件元数据）
        staged: list[_StagedMove] = []     # 已提交的 rename 结算任务（I4）
        budget = self.images.make_request_budget(
            self.config.per_request_processed_budget_bytes)  # I5 请求记账
        outcome = None
        item = None
        try:
            for ordinal, handle in enumerate(handles or []):
                # G6：每张处理前预检（含 pending 预留）——整批白处理前拒绝
                await self.images.check_role_budget(role_id)
                result = await self.images.prepare(role_id, handle,
                                                   charge=budget.charge)
                prepared.append({"ordinal": ordinal, **result})
            for entry in prepared:
                final_path = self.images.storage_path(entry["storage_key"])
                final_path.parent.mkdir(parents=True, exist_ok=True)
                staged.append(self.images.stage_move(
                    entry["temp_path"], final_path))     # owned 结算任务
            for move in staged:
                await move.task_done()
            # ---- 阶段 B：单事务写入（复核+去重重查+配额+认领+条目/资产/引用/账本）----
            # H1：宿主当前 persona/管理员核验在事务准入后、写入前执行
            now = utc_now_iso()
            item = WardrobeItem(
                item_id=f"item_{uuid.uuid4().hex}", role_id=role_id,
                name=name_c, description=desc_c, category=cat_c, tags=tags_c,
                layerable=bool(layerable), source_kind=source_kind,
                created_by_ref=actor_ref, active=True, revision=1,
                created_at=now, updated_at=now)
            new_assets = [
                {"asset_id": p["asset_id"], "processed_sha256": p["sha256"],
                 "storage_key": p["storage_key"], "mime": p["mime"],
                 "byte_size": p["byte_size"], "width": p["width"],
                 "height": p["height"], "ordinal": p["ordinal"],
                 "auto_caption": None,
                 "caption_state": "none", "status": "ready",
                 "created_at": utc_now_iso()}
                for p in prepared]
            outcome = await self.repo.insert_item_verified(
                eff, persona_key, item, new_assets, request_key, fingerprint,
                self._host_pre_commit(host_verify, persona_key, is_host_admin),
                autonomy_check=autonomy_check)
        except TxOutcomeUnknown as exc:
            # I2：提交结果无法确认——不得删除已发布文件（可能已被 DB 引用），
            # 也不得释放预留；等待真实 rename 结算后登记公开核对凭据（I6）
            for move in staged:
                await move.wait_settled()
            credential = None
            if request_key:
                credential = settlement_credential(request_key, fingerprint)
                try:
                    await self.repo.record_settlement_hint(
                        role_id, actor_ref, umo, request_key, fingerprint,
                        "add_item", credential,
                        {"item_id": item.item_id if item else "",
                         "name": name_c,
                         # J2：受信生成的操作资源清单（rid/托管存储键/临时
                         # 文件名全部由服务端生成，不含用户/模型输入）——
                         # 核对确认后据此按真实 DB 引用清理并移交预留
                         "resources": self._resource_entries(prepared, staged)})
                except Exception:
                    credential = None  # 连接不健康：如实不给凭据，保留文件
            if credential:
                exc.credential = credential
            raise
        except BaseException:
            # R5/G4/H5/I4 补偿：owned 所有者——先等全部 rename 真实结算，
            # 再对最终落点/临时路径真实采样清理，完成后才释放预留；
            # 多次取消不中断该补偿，stop 等待其真实完成
            comp = self._bg(self._compensate_add(prepared, staged))
            try:
                await asyncio.shield(comp)
            except asyncio.CancelledError:
                pass  # 等待者取消：补偿所有者继续后台结算
            raise
        # J3：真实业务结果之后的结束阶段（dropped 清理、按 J2 协议逐 rid
        # 移交、附加审计）由持续资源所有者完成——等待者取消后所有者继续，
        # stop 等待其真实完成才关 DB；附加审计失败不把已知 commit 结果
        # 伪装成失败提示
        finalize = self._bg(self._finalize_add_outcome(
            role_id, actor_ref, cat_c, source_kind, outcome, prepared, staged))
        try:
            await asyncio.shield(finalize)
        except asyncio.CancelledError:
            # J3：等待者被取消→如实传播取消；结束阶段所有者继续后台
            # 完成（dropped 清理/rid 移交/审计），stop 等待其真实完成
            raise
        if outcome["status"] == "replayed":
            result = dict(outcome["result"])
            result["idempotent_replay"] = True
            return result
        # H3：首返与账本结果同源同形（repository 统一构造完整规范结果）
        return dict(outcome["result"])

    @staticmethod
    def _resource_entries(prepared: list[dict], staged: list) -> list[dict]:
        """J2：本请求受信资源清单（持久化进核对提示）。

        rid/storage_key/temp 文件名全部由服务端生成；final 路径由存储键
        受控重建，不保存绝对路径。
        """
        entries = []
        for entry in prepared:
            entries.append({
                "rid": entry.get("reservation_id") or "",
                "key": entry.get("storage_key") or "",
                "temp": (entry.get("temp_path").name
                         if entry.get("temp_path") is not None else ""),
            })
        return [e for e in entries if e["rid"] or e["key"] or e["temp"]]

    async def _settle_request_resources(self, role_id: str,
                                        resources: list[dict],
                                        scope: TurnScope | None = None,
                                        persona_key: str | None = None
                                        ) -> list[dict]:
        """J2/J3/K1/L2 共享资源交接协议：在写事务认领下按已结算引用结算。

        K1：引用判据与不可逆清理必须在 `_tx_lock` 写事务内一致完成——
        与 delete/清空/重放等一切写事务互斥，读取视图只含已结算状态，
        不借用其他未提交事务的暂态零引用视图（反例：delete 执行 SQL 未
        commit 时核对误删回滚后仍引用的图）。协议：
        1. 行存在且被 item_images 引用 → 保留文件，事务提交后释放 rid；
        2. 行存在但零引用 → 同一事务内认领（删除该资产行），提交成功
           后 unlink 文件与临时文件——认领先于删除，提交后不可能再被
           去重采纳；文件确认消失才释放 rid；
        3. 行不存在（从未提交/回滚/已被合法删除）→ 提交后 unlink。
        L2：公共核对传入 scope/persona_key——认领事务 BEGIN 后先执行
        `repository.verify_commit` 最终复核（绑定/role_id/世代/窗口授权/
        逐人权限）：初查之后撤权/重绑/降权/清空重建已提交时，旧核对
        StaleContext 拒绝，零清理零结果。内部后台结算（J3）不传 scope，
        不做该复核（已接受操作的必要结算不属于公共维护入口）。
        删除失败（仍存在）保留该 rid（受控待结算，重复调用幂等重试）。
        锁顺序固定 `_tx_lock` → `_pending_lock`；全仓无反向持锁路径。
        O-02：完成判据是受信资源**实际消失**（发布文件与临时文件分别
        采样确认），不是 rid 非空——空 rid 的普通登记键删除失败同样
        保持未完成状态。返回未完成结算的资源描述列表（{rid,key,temp}，
        兼容只有 key、有 rid、临时名与混合；受控待结算，重复调用幂等
        重试）。
        """
        leftover: list[dict] = []
        pending_unlinks: list[tuple[str, str, str]] = []  # (rid, key, temp)
        keep_rids: list[str] = []
        async with self.repo.tx() as conn:
            if scope is not None:
                # L2：认领事务准入后、资源维护前最终作用域复核
                await self.repo.verify_commit(conn, scope, persona_key)
            for res in resources or []:
                rid = res.get("rid") or ""
                key = res.get("key") or ""
                temp_name = res.get("temp") or ""
                referenced = False
                if key:
                    row = await self.repo.get_asset_by_key(key)
                    if row is not None:
                        cur = await conn.execute(
                            "SELECT COUNT(*) AS n FROM item_images"
                            " WHERE asset_id=?", (row["asset_id"],))
                        if (await cur.fetchone())["n"] > 0:
                            referenced = True
                        else:
                            # 认领：删除零引用资产行（与判定同一事务）
                            await conn.execute(
                                "DELETE FROM image_assets"
                                " WHERE role_id=? AND asset_id=?",
                                (role_id, row["asset_id"]))
                if referenced:
                    keep_rids.append(rid)   # 文件保留，提交后释放 rid
                else:
                    pending_unlinks.append((rid, key, temp_name))
        # 认领事务已提交：行删除/保留决定生效，此后的 unlink 不可能
        # 毁坏已恢复（回滚）或后续采纳的引用
        for rid, key, temp_name in pending_unlinks:
            # O-02：发布文件与临时文件各自真实采样，均消失才算完成——
            # 不以 rid 集合为空冒充已退休
            finished = True
            if key:
                finished = self.images.remove_gone(
                    self.images.storage_path(key)) and finished
            if temp_name:
                finished = self.images.remove_gone(
                    self.images.temp_path_of(temp_name)) and finished
            if finished:
                if rid:
                    await self.images.release_pending(rid)
            else:
                leftover.append({"rid": rid, "key": key, "temp": temp_name})
        for rid in keep_rids:
            if rid:
                await self.images.release_pending(rid)
        return leftover

    async def _finalize_add_outcome(self, role_id: str, actor_ref: str,
                                    category: str, source_kind: str,
                                    outcome: dict, prepared: list[dict],
                                    staged: list) -> None:
        """J3：committed/replayed 的结束阶段所有者。

        dropped 即时清理（故障项留给受控恢复）→ 按 J2 协议结算本请求
        资源（被引用保留、冗余真实清理、确认消失才释放 rid）→ 附加审计
        （失败仅吞掉，不覆盖已知业务结果）。
        """
        try:
            for move in staged:
                await move.wait_settled()
            for key in outcome.get("dropped_storage_keys", []):
                try:
                    self.images.remove_if_exists(self.images.storage_path(key))
                except Exception:
                    pass  # 故障：受控恢复兜底，不中断其余清理
            await self._settle_request_resources(
                role_id, self._resource_entries(prepared, staged))
        except Exception:
            pass  # 资源结算异常：预留保持，核对/受控恢复可重试
        try:
            await self.repo.audit(role_id, "add_item", actor_ref,
                                  {"category": category,
                                   "images": outcome["result"]["asset_count"],
                                   "source": source_kind}, "ok")
        except Exception:
            pass  # 附加审计失败不把已提交业务结果伪装成失败

    async def _compensate_add(self, prepared: list[dict], staged: list) -> None:
        """I2/I4/I5：补偿所有者——真实 rename 全部结算后清理文件，再释放预留。

        多次取消/重复取消不中断本所有者；stop 等待其真实完成。清理按
        最终落点与临时路径各做一次真实存在采样（不依赖 future.done()）。
        """
        for move in staged:
            await move.wait_settled()
        for move in staged:
            self.images.remove_if_exists(move.final_path)
            self.images.remove_if_exists(move.temp_path)
        for entry in prepared:
            self.images.discard_temp(entry.get("temp_path"))
            await self.images.release_pending(entry.get("reservation_id") or "")

    async def list_items(self, persona_key: str | None, scope: TurnScope | None,
                         actor_ref: str, umo: str, is_host_admin: bool,
                         page: int = 1, category: str | None = None) -> dict:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "list", is_host_admin, umo, actor_ref,
                "reader")
            if page < 1 or page > 10000:
                raise InvalidInput("页码不合法")
            rows, total = await self.repo.list_items(role_id, page, category,
                                                     LIST_PAGE_SIZE)
            items = []
            for row in rows:
                asset_ids = await self.repo.item_asset_ids(role_id,
                                                           row["item_id"])
                items.append({
                    "item_id": row["item_id"], "name": row["name"],
                    "category": row["category"],
                    "tags": json.loads(row["tags_json"]),
                    "has_image": bool(asset_ids), "image_count": len(asset_ids),
                    "source_kind": row["source_kind"],
                    "is_own": row["created_by_ref"] == actor_ref,
                })
            pages = (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE
            return {"page": page, "pages": max(pages, 1), "total": total,
                    "items": items}

    async def view_item(self, persona_key: str | None, scope: TurnScope | None,
                        actor_ref: str, umo: str, is_host_admin: bool,
                        item_id: str) -> dict:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "view", is_host_admin, umo, actor_ref,
                "reader")
            item_id = clean_text(item_id, field_name="服装ID", max_len=64,
                                 allow_symbols=False)
            row = await self.repo.get_item(role_id, item_id)
            if row is None or not row["active"]:
                raise ItemNotFound("服装不存在")
            asset_ids = await self.repo.item_asset_ids(role_id, item_id)
            assets = []
            for asset_id in asset_ids:
                asset = await self.repo.get_asset(role_id, asset_id)
                if asset:
                    assets.append({
                        "asset_id": asset_id, "mime": asset["mime"],
                        "width": asset["width"], "height": asset["height"],
                        "byte_size": asset["byte_size"],
                        "storage_key": asset["storage_key"],
                        "caption_state": asset["caption_state"],
                        "auto_caption": asset["auto_caption"],
                    })
            return {
                "item_id": row["item_id"], "name": row["name"],
                "description": row["description"], "category": row["category"],
                "tags": json.loads(row["tags_json"]),
                "layerable": bool(row["layerable"]),
                "source_kind": row["source_kind"],
                "is_own": row["created_by_ref"] == actor_ref,
                "revision": row["revision"],
                "assets": assets,
            }

    async def edit_item(self, persona_key: str | None, scope: TurnScope | None,
                        actor_ref: str, umo: str, is_host_admin: bool,
                        item_id: str, fields: dict,
                        host_verify=None) -> dict:
        async with self._request():
            role_id, eff, snap = await self._resolve_and_check(
                persona_key, scope, "view", is_host_admin, umo, actor_ref,
                "contributor")
            item_id = clean_text(item_id, field_name="服装ID", max_len=64,
                                 allow_symbols=False)
            row = await self.repo.get_item(role_id, item_id)
            if row is None or not row["active"]:
                raise ItemNotFound("服装不存在")
            is_owner = row["created_by_ref"] == actor_ref
            if is_owner:
                self.permissions.require_level(snap, PermissionLevel.CONTRIBUTOR)
            else:
                self.permissions.require_level(snap, PermissionLevel.MAINTAINER)
            # R4：未知字段在清洗前明确拒绝（中文字段映射由命令层完成）
            allowed_keys = {"name", "description", "category", "tags", "layerable"}
            unknown = set(fields) - allowed_keys
            if unknown:
                raise InvalidInput(f"未知字段：{'、'.join(sorted(unknown))}；"
                                   "支持 名称/描述/标签/类别")
            updates: dict = {}
            if "name" in fields and fields["name"] is not None:
                updates["name"] = clean_text(fields["name"], field_name="名称",
                                             max_len=NAME_MAX_LEN,
                                             allow_symbols=False)
            if "description" in fields and fields["description"] is not None:
                updates["description"] = clean_text(fields["description"],
                                                    field_name="描述",
                                                    max_len=DESC_MAX_LEN,
                                                    allow_empty=True)
            if "category" in fields and fields["category"] is not None:
                cat = clean_text(fields["category"], field_name="类别",
                                 max_len=12, allow_symbols=False)
                if cat not in ALL_CATEGORIES:
                    raise InvalidInput("类别不合法")
                updates["category"] = cat
            if "tags" in fields and fields["tags"] is not None:
                updates["tags_json"] = json.dumps(clean_tags(fields["tags"]),
                                                  ensure_ascii=False)
            if "layerable" in fields and fields["layerable"] is not None:
                # H9：服务层强校验真实布尔——命令层已按有限拼写表解析，
                # 非布尔值（如字符串 "false"/"0"）一律拒绝，零副作用
                if not isinstance(fields["layerable"], bool):
                    raise InvalidInput(
                        "可叠穿必须是布尔值（是/否/true/false/1/0）")
                updates["layerable"] = 1 if fields["layerable"] else 0
            new_rev = await self.repo.update_item_verified(
                eff, persona_key, role_id, item_id, updates,
                self._host_pre_commit(host_verify, persona_key,
                                      is_host_admin))
            await self.repo.audit(role_id, "edit_item", actor_ref,
                                  {"item": item_id}, "ok")
            return {"item_id": item_id, "revision": new_rev}

    async def delete_item(self, persona_key: str | None, scope: TurnScope | None,
                          actor_ref: str, umo: str, is_host_admin: bool,
                          item_id: str, force: bool = False,
                          host_verify=None) -> dict:
        """删除服装：owner 也至少需要贡献级别；正在穿仅维护者 force 可删。

        H1：delete 同样携带请求级宿主核验（此前缺失）。
        """
        async with self._request():
            role_id, eff, snap = await self._resolve_and_check(
                persona_key, scope, "view", is_host_admin, umo, actor_ref,
                "contributor")
            item_id = clean_text(item_id, field_name="服装ID", max_len=64,
                                 allow_symbols=False)
            row = await self.repo.get_item(role_id, item_id)
            if row is None or not row["active"]:
                raise ItemNotFound("服装不存在")
            is_owner = row["created_by_ref"] == actor_ref
            is_maintainer = snap.level is PermissionLevel.MAINTAINER
            if is_owner:
                self.permissions.require_level(snap, PermissionLevel.CONTRIBUTOR)
            elif not is_maintainer:
                raise PermissionDenied("只能删除自己添加的服装")
            outcome = await self.repo.delete_item_verified(
                eff, persona_key, item_id, force=force, is_owner=is_owner,
                is_maintainer=is_maintainer,
                pre_commit=self._host_pre_commit(host_verify, persona_key,
                                                 is_host_admin))
            removed = 0
            for key in outcome["orphan_keys"]:
                try:
                    path = self.images.storage_path(key)
                    if path.exists():
                        path.unlink()
                        removed += 1
                except Exception:
                    continue
            await self.repo.audit(role_id, "delete_item", actor_ref,
                                  {"item": item_id, "force": bool(force),
                                   "assets_cleaned": removed}, "ok")
            return {"deleted": item_id, "assets_cleaned": removed}

    # ------------------------------------------------------------------
    # 穿搭
    # ------------------------------------------------------------------

    async def wear(self, persona_key: str | None, scope: TurnScope | None,
                   actor_ref: str, umo: str, is_host_admin: bool,
                   item_ids: list[str], expected_revision: int,
                   source_kind: str = "user",
                   request_key: str | None = None,
                   host_verify=None, autonomy_check=None,
                   fingerprint: str | None = None) -> dict:
        """换装（R-02：fingerprint 由调用方显式传入时原样用于账本与
        核对凭据——AI 路径完整规范参数含 expected_revision；不传时
        维持 M1 手工命令既有语义）。"""
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "wear" if source_kind == "user" else "view",
                is_host_admin, umo, actor_ref,
                "contributor" if source_kind == "user" else "reader")
            if len(item_ids) != len(set(item_ids)):
                raise InvalidInput("服装 ID 重复")
            if not item_ids:
                raise InvalidInput("至少选择一件服装")
            cleaned = [clean_text(i, field_name="服装ID", max_len=64,
                                  allow_symbols=False) for i in item_ids]
            entries: list[dict] = []
            for item_id in cleaned:
                row = await self.repo.get_item(role_id, item_id)
                if row is None or not row["active"]:
                    raise ItemNotFound(f"服装 {item_id} 不存在或已删除")
                entries.append({"item_id": item_id, "category": row["category"],
                                "layerable": bool(row["layerable"])})
            slot_map = validate_combination(entries)
            state = await self.repo.get_outfit_state(role_id)
            if state is None:
                raise ItemNotFound("角色穿搭状态缺失")
            lock_until: str | None
            lock_by: str | None
            if source_kind == "user":
                from .models import utc_iso_plus_minutes

                minutes = self.config.manual_lock_minutes
                lock_until = utc_iso_plus_minutes(minutes) if minutes > 0 else None
                lock_by = actor_ref if lock_until else None
            else:
                lock_until = state["manual_lock_until"]
                lock_by = state["manual_lock_by_ref"]
            if fingerprint is None:
                fingerprint = (canonical_fingerprint(
                    "wear", item_ids=cleaned, source=source_kind)
                    if request_key else None)
            try:
                outcome = await self.repo.replace_outfit_verified(
                    eff, persona_key, cleaned, slot_map, expected_revision,
                    source_kind, lock_until, lock_by, request_key, fingerprint,
                    self._host_pre_commit(host_verify, persona_key,
                                          is_host_admin),
                    autonomy_check=autonomy_check)
            except TxOutcomeUnknown as exc:
                # I6：结果未知——登记公开核对凭据（无文件资源，只留提示）
                credential = None
                if request_key:
                    credential = settlement_credential(request_key, fingerprint)
                    try:
                        await self.repo.record_settlement_hint(
                            role_id, actor_ref, umo, request_key, fingerprint,
                            "wear", credential,
                            {"item_ids": list(cleaned),
                             "expected_revision": expected_revision})
                    except Exception:
                        credential = None
                if credential:
                    exc.credential = credential
                raise
            if outcome["status"] == "replayed":
                result = dict(outcome["result"])
                result["idempotent_replay"] = True
                return result
            await self.repo.audit(role_id, "wear", actor_ref,
                                  {"count": len(cleaned), "source": source_kind,
                                   "revision": outcome["result"]["revision"]},
                                  "ok")
            return outcome["result"]

    async def current_outfit(self, persona_key: str | None, scope: TurnScope | None,
                             actor_ref: str, umo: str, is_host_admin: bool) -> dict:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "current", is_host_admin, umo, actor_ref,
                "reader")
            view = await self.outfits.view(role_id)
            named = []
            for entry in view["items"]:
                row = await self.repo.get_item(role_id, entry["item_id"])
                named.append({**entry, "name": row["name"] if row else "(已删除)",
                              "category": row["category"] if row else ""})
            return {**view, "items": named}

    # ------------------------------------------------------------------
    # M2：风格 / 自主开关 / 候选（P-03/P-04）
    # ------------------------------------------------------------------

    async def style_get(self, persona_key: str | None, scope: TurnScope | None,
                        actor_ref: str, umo: str, is_host_admin: bool) -> dict:
        async with self._request():
            role_id, _eff, _snap = await self._resolve_and_check(
                persona_key, scope, "style", is_host_admin, umo, actor_ref,
                "reader")
            profile = await self.styles.get(role_id)
            return {"role_id": role_id, **profile}

    async def style_set(self, persona_key: str | None, scope: TurnScope | None,
                        actor_ref: str, umo: str, is_host_admin: bool,
                        fields: dict, host_verify=None) -> dict:
        """Q-07：仅 provided 字段增量；合并发生在**最终事务内**对最新
        档案——并发的不同字段请求互不抹掉。Q-02：scope 必填 + 请求级
        宿主核验随事务准入执行。"""
        if scope is None:
            from .models import StaleContext

            raise StaleContext("缺少本轮作用域，风格写入拒绝")
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "set_style", is_host_admin, umo, actor_ref,
                "maintainer")
            # 仅对 provided 字段做清洗校验（未出现字段由事务内增量保留）
            updates: dict[str, object] = {}
            if "喜欢" in fields:
                updates["喜欢"] = self.styles.clean_tags_value(fields["喜欢"], "喜欢")
            if "避开" in fields:
                updates["避开"] = self.styles.clean_tags_value(fields["避开"], "避开")
            if "场景" in fields:
                updates["场景"] = self.styles.clean_scene(fields["场景"])
            if "说明" in fields:
                updates["说明"] = self.styles.clean_note(fields["说明"])
            overlap = (set(updates.get("喜欢") or [])
                       & set(updates.get("避开") or []))
            if overlap:
                from .models import InvalidInput

                raise InvalidInput(
                    f"标签不能同时出现在喜欢与避开：{'、'.join(sorted(overlap))}")
            profile = await self.repo.set_style_profile(
                role_id, updates, scope=eff, persona_key=persona_key,
                pre_commit=self._host_pre_commit(host_verify, persona_key,
                                                 is_host_admin))
            await self.repo.audit(role_id, "style_set", actor_ref,
                                  {"revision": profile["revision"],
                                   "fields": sorted(updates)}, "ok")
            return {"role_id": role_id, **profile}

    async def style_candidates(self, persona_key: str | None,
                               scope: TurnScope | None, actor_ref: str,
                               umo: str, is_host_admin: bool,
                               query: str = "", page: int = 1,
                               category: str | None = None) -> dict:
        """风格候选：确定性排序 + 稳定分页（P-04）。返回候选≠已穿。"""
        async with self._request():
            role_id, _eff, _snap = await self._resolve_and_check(
                persona_key, scope, "style", is_host_admin, umo, actor_ref,
                "reader")
            if page < 1 or page > 10000:
                raise InvalidInput("页码不合法")
            rows, _total = await self.repo.list_items(
                role_id, 1, category, self.config.max_items_per_role)
            items = []
            for row in rows:
                asset_ids = await self.repo.item_asset_ids(role_id,
                                                           row["item_id"])
                items.append({
                    "item_id": row["item_id"], "name": row["name"],
                    "description": row["description"] or "",
                    "category": row["category"],
                    "tags": json.loads(row["tags_json"] or "[]"),
                    "has_image": bool(asset_ids),
                })
            profile = await self.styles.get(role_id)
            result = self.styles.rank(items, profile, query, page)
            return {"role_id": role_id, **result}

    async def autonomy_get(self, persona_key: str | None,
                           scope: TurnScope | None, actor_ref: str,
                           umo: str, is_host_admin: bool) -> dict:
        from .autonomy import describe as describe_autonomy

        async with self._request():
            role_id, _eff, _snap = await self._resolve_and_check(
                persona_key, scope, "autonomy", is_host_admin, umo, actor_ref,
                "reader")
            view = await describe_autonomy(self, role_id, umo)
            return {"role_id": role_id, **view}

    async def autonomy_set(self, persona_key: str | None,
                           scope: TurnScope | None, actor_ref: str,
                           umo: str, is_host_admin: bool,
                           updates: dict, host_verify=None) -> dict:
        if scope is None:
            from .models import StaleContext

            raise StaleContext("缺少本轮作用域，自主开关写入拒绝")
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "set_autonomy", is_host_admin, umo,
                actor_ref, "maintainer")
            await self.repo.set_autonomy(
                role_id, master=updates.get("master"),
                add=updates.get("add"), wear=updates.get("wear"),
                scope=eff, persona_key=persona_key,
                pre_commit=self._host_pre_commit(host_verify, persona_key,
                                                 is_host_admin))
            await self.repo.audit(role_id, "autonomy_set", actor_ref,
                                  {k: v for k, v in updates.items()}, "ok")
            from .autonomy import describe as describe_autonomy

            view = await describe_autonomy(self, role_id, umo)
            return {"role_id": role_id, **view}

    async def unlock(self, persona_key: str | None, scope: TurnScope | None,
                     actor_ref: str, umo: str, is_host_admin: bool,
                     host_verify=None) -> None:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "unlock", is_host_admin, umo, actor_ref,
                "maintainer")
            await self.outfits.unlock(
                role_id, eff, persona_key,
                self._host_pre_commit(host_verify, persona_key,
                                      is_host_admin))
            await self.repo.audit(role_id, "unlock", actor_ref, {}, "ok")

    # ------------------------------------------------------------------
    # 图片展示（命令层用 storage_key 发图；工具层绝不返回路径）
    # ------------------------------------------------------------------

    async def item_image_path(self, persona_key: str | None, scope: TurnScope | None,
                              actor_ref: str, umo: str, is_host_admin: bool,
                              asset_id: str) -> Path:
        async with self._request():
            role_id, eff, _snap = await self._resolve_and_check(
                persona_key, scope, "view_image", is_host_admin, umo, actor_ref,
                "reader")
            asset = await self.repo.get_asset(role_id, asset_id)
            if asset is None:
                raise ItemNotFound("图片不存在或不属于该角色")
            return self.images.storage_path(asset["storage_key"])

    async def verify_settlement(self, persona_key: str | None,
                                scope: TurnScope | None, actor_ref: str,
                                umo: str, is_host_admin: bool,
                                credential: str) -> dict:
        """I6：公开原操作只读核对（reader 级；不再次执行、不占额度）。

        走正常身份/授权/绑定/世代解析后按凭据定位原请求：committed 返回
        账本原结果；确定未提交如实说明；在途/不可判定返回待核对。凭据
        必须与原角色/窗口/脱敏发起者一致，否则统一拒绝。
        """
        async with self._request():
            try:
                role_id, eff, _snap = await self._resolve_and_check(
                    persona_key, scope, "verify", is_host_admin, umo,
                    actor_ref, "reader")
            except (RoleUnbound, NotInitialized):
                # K1：另一笔写事务（如清空）尚未结算时，共用连接的身份
                # 解析读到暂态视图——不能把未提交状态当结论泄露；连接仍
                # 有未决事务时如实返回待核对，结算后重试即恢复
                from .repository import tx_state

                db = self.repo._require()
                if await tx_state(db) is not False:
                    return {"state": "pending", "action": "verify"}
                raise
            token = clean_text(credential, field_name="核对凭据", max_len=64,
                               allow_symbols=False)
            # L1：结果判定与尝试清单读取在 repo 的已结算写事务内完成
            #（返回即已提交视图），随后才进入资源认领
            outcome = await self.repo.verify_settlement(role_id, umo,
                                                        actor_ref, token)
            hint = outcome.pop("hint", None) or {}
            if outcome["state"] in ("committed", "not_committed"):
                # L2：资源认领事务内以初查 eff 做最终作用域复核——初查
                # 之后撤权/重绑/降权/清空重建已提交时旧核对拒绝，零清理
                # 零结果（文件/预留保持可达）。J2：确认后按真实 DB 引用
                # 结算本请求文件——被引用保留、未采用/冗余真实清理、
                # 确认消失才释放 rid；失败项保持受控待结算
                try:
                    await self._settle_request_resources(
                        role_id, hint.get("resources") or [],
                        scope=eff, persona_key=persona_key)
                except StaleContext:
                    raise  # 旧 scope 已失效：拒绝且不清理（L2）
            return outcome

    # ------------------------------------------------------------------
    # M2 预留：受控查询/添衣/选穿接口（不注册工具，不做后台任务）
    # ------------------------------------------------------------------
