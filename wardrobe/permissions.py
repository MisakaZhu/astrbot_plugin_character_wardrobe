"""窗口授权与逐人权限判定（R2 整改）。

模型（TECHNICAL §4）：
- 窗口授权（session_grants 行）= 该窗口可访问（read 基线）+ AI 变更开关；
- 贡献/维护是**逐 sender** 集合（sender_grants），不随窗口级别扩散；
- 宿主真实管理员在其所在授权窗口提升为维护者；模型或普通参数提供的
  "is_admin" 无效——该标志只能来自宿主事件（build_identity）。
- 每次快照捕获授权世代（auth_events 单调序列）；撤销/重授/清空/重绑后
  旧 TurnScope 在最终提交事务中被 verify_commit 拒绝。
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import (
    NotInitialized,
    PermissionDenied,
    PermissionLevel,
    StaleContext,
)

READ_ACTIONS = frozenset({"list", "view", "current", "view_image",
                          "verify", "style", "autonomy"})
CONTRIB_ACTIONS = frozenset({"add", "edit_own", "delete_own", "wear"})
MAINTAIN_ACTIONS = frozenset({
    "edit_any", "delete_any", "force_delete", "grant", "revoke", "unlock",
    "clear", "set_style", "set_autonomy", "rebind", "disable_role",
})

_LEVEL_ORDER = {PermissionLevel.READER: 0, PermissionLevel.CONTRIBUTOR: 1,
                PermissionLevel.MAINTAINER: 2}


@dataclass(frozen=True)
class GrantSnapshot:
    role_id: str
    umo: str
    sender_ref: str
    level: PermissionLevel
    allow_ai_mutation: bool
    revision: int
    auth_generation: int


class PermissionService:
    def __init__(self, repository) -> None:
        self._repo = repository

    async def snapshot(self, role_id: str, umo: str, sender_ref: str,
                       is_host_admin: bool,
                       expected_grant_revision: int | None = None,
                       expected_auth_generation: int | None = None,
                       ) -> GrantSnapshot:
        """取窗口+逐人授权快照；宿主管理员提升为维护者。"""
        row = await self._repo.get_grant(role_id, umo)
        if row is None:
            raise NotInitialized(
                "当前窗口尚未授权访问该角色衣柜；请由管理者执行 /衣柜 初始化 或 授权")
        if (expected_grant_revision is not None
                and row["revision"] != expected_grant_revision):
            raise StaleContext(
                "窗口授权在请求执行期间被修改，旧请求已作废；请重新发起")
        generation = await self._repo.auth_generation()
        if (expected_auth_generation is not None
                and generation != expected_auth_generation):
            raise StaleContext(
                "授权世代在请求执行期间被推进（撤销/重授/清空/重绑），"
                "旧请求已作废；请重新发起")
        if is_host_admin:
            level = PermissionLevel.MAINTAINER
        else:
            sender_level = await self._repo.get_sender_level(
                role_id, umo, sender_ref)
            level = PermissionLevel(sender_level) if sender_level in (
                "reader", "contributor", "maintainer") else PermissionLevel.READER
        return GrantSnapshot(
            role_id=role_id,
            umo=umo,
            sender_ref=sender_ref,
            level=level,
            allow_ai_mutation=bool(row["allow_ai_mutation"]),
            revision=row["revision"],
            auth_generation=generation,
        )

    def require_level(self, snapshot: GrantSnapshot,
                      minimum: PermissionLevel) -> None:
        if _LEVEL_ORDER[snapshot.level] < _LEVEL_ORDER[minimum]:
            raise PermissionDenied("当前窗口内你的权限不足")

    def check_action(self, snapshot: GrantSnapshot, action: str) -> None:
        if action in READ_ACTIONS:
            return
        if action in CONTRIB_ACTIONS:
            self.require_level(snapshot, PermissionLevel.CONTRIBUTOR)
            return
        if action in MAINTAIN_ACTIONS:
            self.require_level(snapshot, PermissionLevel.MAINTAINER)
            return
        raise PermissionDenied(f"未知操作 {action}")
