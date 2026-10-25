"""稳定角色与 persona 映射。

服务端从最终生效宿主人格解析角色；模型与用户文字不能指定身份。
未知 / 未设置 / "[%None]" 人格一律拒绝，不落入公共角色。
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import (
    RoleUnbound,
    StaleContext,
    utc_now_iso,
)

# 宿主明确的“无人格”哨兵值（persona_mgr.py 处理的 conversation 哨兵）
NONE_PERSONA_SENTINELS = frozenset({"[%None]", "", "default_unset_placeholder"})


def decide_persona_key(persona_id: str | None) -> str | None:
    """把宿主解析结果换算为稳定 persona 键；不可解析时返回 None（必须拒绝）。"""
    if not isinstance(persona_id, str):
        return None
    key = persona_id.strip()
    if not key or key in NONE_PERSONA_SENTINELS:
        return None
    return key


@dataclass(frozen=True)
class RoleContext:
    role_id: str
    binding_revision: int
    display_name: str


class RoleResolver:
    """persona key → 稳定 role_id 的映射与复核。"""

    def __init__(self, repository) -> None:  # repository: wardrobe.repository.Repository
        self._repo = repository

    async def resolve(self, persona_key: str | None,
                      expected_binding_revision: int | None = None,
                      expected_role_id: str | None = None) -> RoleContext:
        """解析并复核绑定。expected 不匹配说明绑定已变，旧请求必须作废。"""
        key = decide_persona_key(persona_key)
        if key is None:
            raise RoleUnbound(
                "当前会话没有生效角色人格；请先在宿主设置 persona 并执行 /衣柜 初始化")
        row = await self._repo.get_binding(key)
        if row is None:
            raise RoleUnbound(
                f"人格 {key} 尚未绑定衣柜角色；请由管理者执行 /衣柜 初始化 或 授权重绑定")
        if not row["role_enabled"]:
            raise RoleUnbound("该角色已停用")
        if (expected_role_id is not None
                and row["role_id"] != expected_role_id):
            raise StaleContext(
                "请求针对的角色与当前绑定不一致（重绑/清空重建），旧请求已作废；请重新发起")
        if (expected_binding_revision is not None
                and row["binding_revision"] != expected_binding_revision):
            raise StaleContext(
                "角色绑定在请求执行期间发生变化（重命名/重绑定），旧请求已作废；请重新发起")
        return RoleContext(
            role_id=row["role_id"],
            binding_revision=row["binding_revision"],
            display_name=row["role_display"],
        )

    async def initialize(self, persona_key: str | None, display_name: str,
                         actor_ref: str, umo_ref: str,
                         pre_commit=None) -> RoleContext:
        key = decide_persona_key(persona_key)
        if key is None:
            raise RoleUnbound("当前会话没有可绑定的生效人格，无法初始化角色")
        role_id, binding_rev, _grant_rev = await self._repo.create_role(
            display_name, actor_ref, key, umo_ref, pre_commit=pre_commit)
        return RoleContext(role_id=role_id, binding_revision=binding_rev,
                           display_name=display_name)

    async def rebind(self, persona_key: str, role_id: str, actor_ref: str) -> RoleContext:
        key = decide_persona_key(persona_key)
        if key is None:
            raise RoleUnbound("目标人格键无效")
        binding_rev = await self._repo.rebind_persona(key, role_id, actor_ref)
        return RoleContext(role_id=role_id, binding_revision=binding_rev,
                           display_name=role_id)
