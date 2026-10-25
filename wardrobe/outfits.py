"""有效衣服组合、当前穿搭与手工锁。

- 单次至多 8 件；重复 / 跨角色 / 已删除 / 类别冲突 / 过期版本拒绝。
- 错误不泄漏其他角色库存信息。
- 手工锁默认 30 分钟（可配）；锁期间 AI 选穿拒绝，手工新选择可更新锁。
"""

from __future__ import annotations

from datetime import datetime, timezone

from .config import OUTFIT_MAX_ITEMS, WardrobeConfig
from .models import (
    MUTEX_GROUPS,
    SINGLE_SLOT_CATEGORIES,
    InvalidInput,
    ItemNotFound,
    ManualLockActive,
    parse_utc_iso,
    utc_iso_plus_minutes,
)


def validate_combination(entries: list[dict]) -> dict[str, str]:
    """校验组合，返回 item_id -> slot（类别）映射。

    entries: [{item_id, category, layerable}]；重复 item_id 由调用方先行拒绝。
    冲突时抛 InvalidInput，不携带其他角色信息。
    """
    if not entries:
        raise InvalidInput("至少选择一件服装")
    if len(entries) > OUTFIT_MAX_ITEMS:
        raise InvalidInput(f"单次至多穿 {OUTFIT_MAX_ITEMS} 件")
    slot_map: dict[str, str] = {}
    category_counts: dict[str, list[str]] = {}
    for entry in entries:
        item_id = entry["item_id"]
        category = entry["category"]
        if item_id in slot_map:
            raise InvalidInput("同一件服装不能重复选择")
        slot_map[item_id] = category
        category_counts.setdefault(category, []).append(item_id)
    for category, ids in category_counts.items():
        if category in SINGLE_SLOT_CATEGORIES and len(ids) > 1:
            layerable_count = sum(
                1 for e in entries if e["category"] == category and e.get("layerable"))
            if len(ids) > 2 or layerable_count < len(ids):
                raise InvalidInput(f"类别「{category}」至多一件（可叠穿服装至多两件）")
    for group in MUTEX_GROUPS:
        present = sorted(c for c in group if c in category_counts)
        if len(present) > 1:
            raise InvalidInput(f"类别冲突：{'、'.join(present)} 不能同时穿着")
    return slot_map


def lock_active(lock_until: str | None, now: datetime | None = None) -> bool:
    if not lock_until:
        return False
    until = parse_utc_iso(lock_until)
    if until is None:
        return False
    now = now or datetime.now(timezone.utc)
    return until > now


class OutfitService:
    def __init__(self, repository, config: WardrobeConfig) -> None:
        self._repo = repository
        self._config = config

    async def view(self, role_id: str) -> dict:
        state = await self._repo.get_outfit_state(role_id)
        items = await self._repo.get_outfit_items(role_id)
        return {
            "revision": state["revision"] if state else 0,
            "items": [
                {"item_id": r["item_id"], "slot": r["slot"],
                 "active": bool(r["active"]) if r["active"] is not None else False}
                for r in items
            ],
            "source_kind": state["source_kind"] if state else "manual",
            "manual_lock_until": state["manual_lock_until"] if state else None,
            "updated_at": state["updated_at"] if state else None,
        }

    def _manual_lock_deadline(self) -> str | None:
        minutes = self._config.manual_lock_minutes
        if minutes <= 0:
            return None
        return utc_iso_plus_minutes(minutes)

    async def select(self, role_id: str, item_ids: list[str], expected_revision: int,
                     source_kind: str, actor_ref: str) -> dict:
        """原子保存当前穿搭。手工来源更新锁；AI 来源被锁拒绝。"""
        if len(item_ids) != len(set(item_ids)):
            raise InvalidInput("服装 ID 重复")
        if not item_ids:
            raise InvalidInput("至少选择一件服装")
        state = await self._repo.get_outfit_state(role_id)
        if state is None:
            raise ItemNotFound("角色穿搭状态缺失")
        if lock_active(state["manual_lock_until"]) and source_kind == "ai":
            raise ManualLockActive(
                "手工穿搭锁生效中，AI 选穿已拒绝；请等待锁过期或由管理者解锁",
                until=state["manual_lock_until"])
        entries: list[dict] = []
        for item_id in item_ids:
            row = await self._repo.get_item(role_id, item_id)
            if row is None or not row["active"]:
                raise ItemNotFound(f"服装 {item_id} 不存在或已删除")
            entries.append({"item_id": item_id, "category": row["category"],
                            "layerable": bool(row["layerable"])})
        slot_map = validate_combination(entries)
        lock_until = (self._manual_lock_deadline()
                      if source_kind == "user" else state["manual_lock_until"])
        lock_by = (actor_ref if (source_kind == "user" and lock_until)
                   else state["manual_lock_by_ref"])
        new_revision = await self._repo.replace_outfit(
            role_id, expected_revision,
            [(item_id, slot_map[item_id]) for item_id in item_ids],
            source_kind, lock_until, lock_by)
        return {"revision": new_revision, "item_ids": list(item_ids),
                "source_kind": source_kind}

    async def unlock(self, role_id: str, scope=None,
                     persona_key: str | None = None,
                     pre_commit=None) -> None:
        """G2/H1：解锁透传 scope 与事务准入宿主核验钩子。"""
        await self._repo.clear_manual_lock(role_id, scope, persona_key,
                                           pre_commit)
