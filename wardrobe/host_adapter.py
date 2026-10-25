"""宿主适配：事件身份、persona 解析、来源句柄注册与临时投影适配。

本模块是 AstrBot 宿主与领域层的边界。宿主相关 import 一律函数内延迟，
保证核心包在无宿主环境下可完整测试。纯逻辑（persona 键判定、句柄注册表、
路径边界）在此实现并有独立测试。

已核实宿主事实（官方 v4.28.0，a412146401426c0cdff8bbefb8627a03da519da8）：
- PersonaManager.resolve_selected_persona(umo, conversation_persona_id,
  platform_name, provider_settings) -> (persona_id, persona, forced, webchat)
  [astrbot/core/persona_mgr.py L83]
- 命令路径：conversation_manager.get_curr_conversation_id(umo) →
  get_conversation(umo, cid).persona_id，复刻宿主同一解析链
  [astrbot/core/astr_main_agent.py _ensure_persona_and_skills]
- ContentPart.mark_as_temp() 置 _no_save；序列化保留；历史落库时排除
  [astrbot/core/agent/message.py L19/L345]
- ProviderRequest.extra_user_content_parts 经 assemble_context() 逐件
  model_dump_for_context() [astrbot/core/provider/entities.py L100/L203]
- Image 组件字段 file/url/path；消息组件来自 astrbot/core/message/components.py
- on_llm_response 由 on_agent_done 派发。任务#017 T-01 精确分列：
  - **可达 on_llm_response**：正常完成（含空回复/正常流式——
    tool_loop_agent_runner L180-203）、用户显式 request_stop 中断
    （L1491-1518）、普通 Exception 逃逸到 run_agent 时 run_util
    L317-352 另有 on_agent_done 补派发；
  - **缺可信 on_llm_response 覆盖**：provider 异常经 fallback 转
    role="err"（L643-649）后 step 转 ERROR 并正常 return（L894-912，
    不进 on_agent_done）；外层 Task.cancel 经 _await_or_stop
    （L463-498）传播 CancelledError（与 request_stop 是不同路径）；
    CW 之后的请求 hook 停止事件使 internal 在 reset 前 return
    （L277-284）；
  - **after 与 response 是两个回调**：空结果/正常流式跳过
    after_message_sent（respond/stage L169）只说明无 after，单独不
    证明没有 on_llm_response——不列入缺覆盖依据
    [astr_agent_hooks L26-35、context_utils L95-101]。
  结论：三类缺覆盖路径足以证明宿主不构成每请求可信结束契约。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from .models import (
    HandleInvalid,
    ResolvedIdentity,
    SourceHandle,
    TurnScope,
    masked_actor_ref,
)

HANDLE_TTL_TURNS = 1
# Q-06：句柄有限 TTL（秒）——注册/取用时惰性清扫过期条目（有界、不忙
# 循环）；过期句柄取用一律拒绝。
HANDLE_TTL_SECONDS = 600

# 任务#016 S-01 / #017 T-01：固定宿主（v4.28.0/a4121464）不存在**每
# 请求可信结束契约**——自动投影与 AI 图片句柄暴露按任务#012/#015/
# #016 安全降级条款关闭。逐分支依据（固定快照静态核实，不笼统概括
# "所有异常"）：
# - 可达 on_llm_response：正常完成（含空回复/正常流式，runner
#   L180-203）、用户显式 request_stop 中断（L1491-1518）、普通
#   Exception 逃逸到 run_agent 时 run_util L317-352 另有 on_agent_done
#   补派发；
# - 缺可信 on_llm_response 覆盖（三类，足以证明不能自动投影）：
#   provider 异常经 fallback 转 role="err"（L643-649）后 step 转
#   ERROR 并正常 return（L894-912，不进 on_agent_done）；外层
#   Task.cancel 经 _await_or_stop（L463-498）传播 CancelledError（与
#   request_stop 是不同路径）；CW 之后的请求 hook 停止事件使
#   internal 在 reset 前 return（L277-284）；
# - after 与 response 是两个回调：空结果/正常流式跳过 after
#   （respond/stage L169）只说明无 after，单独不证明没有
#   on_llm_response——不列入缺覆盖依据（任务#017 T-01 纠正：此前
#   文案把它并列为无回调依据，表达过宽）。
# extra_user_content_parts 字段与 on_llm_response 装饰器的存在只证明
# 字段和事件存在，不构成每请求生命周期合约；注入时也无法预判本请求
# 将走哪条路径。因此自动投影关闭（显式查询/手工/风格/已保存
# 开关继续），不由用户配置绕过。
AUTO_PROJECTION_UNSUPPORTED_REASON = (
    "宿主无每请求可信结束契约（provider错误role=err经ERROR返回、外层"
    "任务取消、后置hook停止事件三类路径无结束回调），自动投影与AI图片"
    "句柄暴露已安全关闭；查询与手工功能不受影响")


def auto_projection_support() -> tuple[bool, str]:
    """评估本宿主是否具备自动投影所需的每请求可信结束契约。

    返回 (是否支持, 原因)。固定基线 a4121464 静态核实为不支持
    （理由见 AUTO_PROJECTION_UNSUPPORTED_REASON）——关闭是任务书授权
    的安全降级，不是产品完成声明；未来宿主须先证明覆盖正常/错误
    返回/异常/外层取消/后置hook停止/流式/请求复用的完整合约才可
    启用。
    """
    return False, AUTO_PROJECTION_UNSUPPORTED_REASON


def _now_ts() -> float:
    import time as _time

    return _time.time()


@dataclass
class HandleRegistry:
    """本轮图片句柄注册表。

    句柄只由宿主适配器从真实消息组件创建；get 时逐字段复核绑定。
    turn 结束由 main.py 调用 expire_turn 清理。
    """

    namespace: str
    _handles: dict[str, SourceHandle] = field(default_factory=dict)

    def purge_expired(self, now_ts: float | None = None) -> int:
        """Q-06：惰性清扫超过 TTL 的句柄（调用点：注册/取用/收尾）。

        返回清除数量；不忙循环、不占锁——句柄表只在本插件事件循环内
        访问。"""
        import time as _time

        now = now_ts if now_ts is not None else _time.time()
        stale = [k for k, v in self._handles.items()
                 if getattr(v, "created_ts", 0) and now - v.created_ts
                 > HANDLE_TTL_SECONDS]
        for k in stale:
            self._handles.pop(k, None)
        return len(stale)

    def register_from_components(
        self, turn_token: str, umo: str, sender_ref: str, role_id: str,
        sender_id: str, platform: str,
        image_specs: list[dict], allow_reply: bool,
    ) -> list[SourceHandle]:
        """从消息组件规格注册句柄。

        image_specs: [{kind: base64|local|remote, payload, from_reply: bool}]
        来自 main.py 对真实 Image 组件的解读；from_reply 组件仅在
        allow_reply=True（管理员开启且适配器可信）时接受。
        """
        self.purge_expired()
        created: list[SourceHandle] = []
        for index, spec in enumerate(image_specs):
            if spec.get("from_reply") and not allow_reply:
                continue
            kind = spec.get("kind")
            payload = spec.get("payload") or ""
            if kind not in ("base64", "local", "remote") or not payload:
                continue
            handle = SourceHandle(
                handle_id=f"hd_{uuid.uuid4().hex}",
                turn_token=turn_token,
                umo=umo,
                sender_ref=sender_ref,
                role_id=role_id,
                kind=kind,
                payload=payload,
                component_index=index,
            )
            handle.created_ts = _now_ts()
            self._handles[handle.handle_id] = handle
            created.append(handle)
        return created

    def get_verified(self, handle_id: str, turn_token: str, umo: str,
                     sender_ref: str, role_id: str) -> SourceHandle:
        """取回句柄：任何绑定字段不匹配即拒绝，过期即拒绝。"""
        self.purge_expired()
        handle = self._handles.get(handle_id)
        if handle is None:
            raise HandleInvalid("图片句柄不存在或已过期")
        if getattr(handle, "created_ts", 0) and                 _now_ts() - handle.created_ts > HANDLE_TTL_SECONDS:
            self._handles.pop(handle_id, None)
            raise HandleInvalid("图片句柄已超过有限有效期")
        if (handle.turn_token != turn_token or handle.umo != umo
                or handle.sender_ref != sender_ref or handle.role_id != role_id):
            raise HandleInvalid("图片句柄与当前请求不匹配")
        return handle

    def expire_turn(self, turn_token: str | None = None) -> None:
        if turn_token is None:
            self._handles.clear()
            return
        self._handles = {k: v for k, v in self._handles.items()
                         if v.turn_token != turn_token}

    def expire_handle(self, handle_id: str) -> None:
        """精确清理单个句柄（数量上限截断用；不影响其他 turn/句柄）。"""
        self._handles.pop(handle_id, None)


def build_identity(*, umo: str, platform_name: str, sender_id: str,
                   persona_id: str | None, persona_display: str | None,
                   is_host_admin: bool, message_id: str,
                   namespace: str) -> ResolvedIdentity:
    """把宿主事件值收敛为可信身份；sender 身份立即脱敏。

    G2：空 sender（无法获取可靠事件身份）必须拒绝——空串若参与脱敏哈希
    会收敛为固定 ref，使无身份请求“成为”同一共同作者。
    """
    if not umo or not platform_name or not message_id:
        raise HandleInvalid("事件缺少可信标识（umo/platform/message_id）")
    if not sender_id or not str(sender_id).strip():
        raise HandleInvalid("事件缺少真实发送者身份，无法确定操作者")
    return ResolvedIdentity(
        umo=umo,
        platform_name=platform_name,
        sender_ref=masked_actor_ref(namespace, str(sender_id), platform_name),
        persona_key=persona_id,
        persona_display=persona_display,
        is_host_admin=bool(is_host_admin),
        message_id=message_id,
    )


def build_scope(role_id: str, binding_revision: int, grant_revision: int,
                message_id: str, action_nonce: str = "") -> TurnScope:
    """构造本轮作用域。turn_token 绑定消息与动作，重放/重试可区分。"""
    token = f"{message_id}:{action_nonce}:{uuid.uuid4().hex[:8]}"
    return TurnScope(role_id=role_id, binding_revision=binding_revision,
                     grant_revision=grant_revision, turn_token=token)


def classify_image_component(file_value: str | None, url_value: str | None,
                             path_value: str | None) -> dict | None:
    """把宿主 Image 组件字段归类为句柄规格。

    base64:// 前缀 → base64；http(s) URL → remote；file:// URI、Windows 盘符
    绝对路径（X:\\...）与 POSIX 绝对路径（/...）→ local（须落在受信媒体根）。
    UNC（\\\\...）与其余形态返回 None（拒绝）。
    """
    raw = (file_value or "").strip()
    url = (url_value or "").strip()
    path = (path_value or "").strip()
    if raw.startswith("base64://"):
        return {"kind": "base64", "payload": raw[len("base64://"):]}
    if url.startswith(("http://", "https://")):
        return {"kind": "remote", "payload": url}
    if raw.startswith(("http://", "https://")):
        return {"kind": "remote", "payload": raw}
    for candidate in (path, raw):
        if candidate.startswith("file://"):
            return {"kind": "local", "payload": candidate[7:]}
        if len(candidate) > 2 and candidate[1] == ":" and candidate[0].isalpha():
            return {"kind": "local", "payload": candidate}      # Windows 盘符
        if candidate.startswith("/"):
            return {"kind": "local", "payload": candidate}      # POSIX 绝对
        if candidate.startswith("\\\\"):
            return None  # UNC 路径不接受
    return None
