"""角色小衣柜插件入口（薄层）。

职责仅限：宿主生命周期、命令注册与解析、事件→可信身份转换、
把领域结果转为宿主消息。全部业务逻辑在 wardrobe/ 包与 service。

R1 整改：
- `_resolve_identity` 只做"最终 persona + 真实身份"解析（空库首次初始化
  不再要求已存在角色）；`_resolve_scope` 另行做角色/授权复核并构造
  不可伪造 TurnScope（含 sender_ref 与授权世代）。
- 全部子命令把最终 persona、TurnScope、真实 sender 传给服务层，
  不再以 None 替代；persona_display 未定义缺陷已修复。
- 尾参使用官方 GreedyStr（astrbot.core.star.filter.command）——普通
  `rest: str = ""` 按官方 CommandFilter 只绑定一个 token。
R2 整改：sender_ref 使用 DB 持久 installation_namespace；授权命令支持
逐人贡献/维护与窗口启用两级。
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

# 官方 4.26.7（fed29848）的 astrbot.api 根不导出事件类型；
# AstrMessageEvent 由 astrbot.api.event 提供，logger 由根提供。
from astrbot.api import logger

# 包内导入闭合（#026）：宿主以 data.plugins.<plugin>.main 包式加载本模块，
# 此时顶层 `wardrobe` 不可导入（插件根不在 sys.path），必须用包内相对导入；
# 本地替身测试以"插件根进 sys.path + import main"顶层加载，__package__ 为空。
# 两种模式绑定的是同一个本插件 wardrobe 包，类型对象同一；不走
# try/except ImportError，避免吞掉包内真实的依赖导入错误。
if __package__:
    from .wardrobe.autonomy import parse_switch_args
    from .wardrobe.config import WardrobeConfig
    from .wardrobe.host_adapter import (
        HandleRegistry,
        auto_projection_support,
        build_identity,
        build_scope,
        classify_image_component,
    )
    from .wardrobe.llm_tools import (
        tool_add_item,
        tool_get_state,
        tool_list_items,
        tool_wear_items,
    )
    from . import wardrobe as _wardrobe_pkg
    from .wardrobe.models import (
        PermissionLevel,
        ServiceStopping,
        TurnScope,
        TxOutcomeUnknown,
        WardrobeError,
        masked_actor_ref,
    )
    from .wardrobe.projection import ProjectionManager
    from .wardrobe.service import WardrobeService
    from .wardrobe.styles import parse_style_args
else:
    from wardrobe.autonomy import parse_switch_args
    from wardrobe.config import WardrobeConfig
    from wardrobe.host_adapter import (
        HandleRegistry,
        auto_projection_support,
        build_identity,
        build_scope,
        classify_image_component,
    )
    from wardrobe.llm_tools import (
        tool_add_item,
        tool_get_state,
        tool_list_items,
        tool_wear_items,
    )
    import wardrobe as _wardrobe_pkg
    from wardrobe.models import (
        PermissionLevel,
        ServiceStopping,
        TurnScope,
        TxOutcomeUnknown,
        WardrobeError,
        masked_actor_ref,
    )
    from wardrobe.projection import ProjectionManager
    from wardrobe.service import WardrobeService
    from wardrobe.styles import parse_style_args
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain, Reply
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.star.filter.command import GreedyStr

PLUGIN_NAME = "astrbot_plugin_character_wardrobe"

# 编辑字段中文名 → 服务字段（R4；未知字段由服务层明确拒绝）
_EDIT_FIELD_MAP = {
    "名称": "name",
    "描述": "description",
    "标签": "tags",
    "类别": "category",
    "可叠穿": "layerable",
}

_GRANT_LEVELS = {"贡献": "contributor", "管理": "maintainer", "读取": None}

# 类别 token 集合（首 token 显式类别形态）
_ALL_CATEGORY_TOKENS = {"上装", "下装", "连衣裙", "外套", "鞋", "套装", "配饰"}

# H9：可叠穿有限拼写表（中/英/数值），清洗后转为真正 bool；其余拒绝
_LAYERABLE_TRUE = frozenset({"true", "是", "1", "开", "on", "yes", "y"})
_LAYERABLE_FALSE = frozenset({"false", "否", "0", "关", "off", "no", "n"})


def parse_layerable_value(value: str) -> bool:
    """把可叠穿字段值按有限拼写表解析为 bool；空/未知/非法拒绝。"""
    InvalidInput = _wardrobe_pkg.models.InvalidInput

    token = str(value).strip().lower()
    if token in _LAYERABLE_TRUE:
        return True
    if token in _LAYERABLE_FALSE:
        return False
    raise InvalidInput(
        "可叠穿只能是 是/否、true/false 或 1/0；空值与其他写法均拒绝")


class CharacterWardrobePlugin(Star):
    """AstrBot 插件入口。"""

    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context)
        self._config: WardrobeConfig | None = None
        self._service: WardrobeService | None = None
        self._handles = HandleRegistry(namespace="pending")
        self._projection: ProjectionManager | None = None
        self._raw_config = config or {}
        # R-01：整个已接受 hook/工具请求的停用所有权——在途任务登记 +
        # 停止准入位。terminate 先置准入、等在途真实完成，再清理并最后
        # 关 DB；不使用模块级单例，双实例天然隔离。
        self._closing = False
        self._active_requests: set[asyncio.Task] = set()
        # 任务#016 S-01：自动投影能力判定（初始化时评估）。固定宿主
        # a4121464 无每请求可信结束契约 → 关闭（False），关闭发生在
        # on_llm_request 的任何登记/句柄注册/读取/注入之前。
        self._auto_projection_enabled = False
        self._auto_projection_reason = ""

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def initialize(self, config: dict | None = None) -> None:
        raw = dict(self._raw_config)
        if config:
            raw.update(config)
        self._config = WardrobeConfig.from_dict(raw)
        data_root = Path(StarTools.get_data_dir(PLUGIN_NAME))
        self._service = WardrobeService(
            data_root, self._config,
            trusted_media_roots=[Path(p) for p in raw.get("trusted_media_roots", []) or []])
        await self._service.start()
        # R2/H8：句柄注册表使用 DB 持久 namespace；start 已核验/补齐并
        # 保证非空（缺失且不可恢复时 start 失败），不存在 "pending" 回退
        self._handles = HandleRegistry(namespace=self._service.repo.namespace)
        # R-01：投影记录被丢弃时按记录在案 handle_id 同步清偿句柄——
        # part/句柄/短期记录同批归零；回调只在记录真实释放时触发，
        # 不影响在途请求与并发同 turn 新注册的句柄。
        self._projection = ProjectionManager(
            handle_max=self._config.autonomy_handle_max,
            max_items=self._config.autonomy_projection_max_items,
            max_chars=self._config.autonomy_projection_max_chars,
            on_release=self._release_record_handles)
        # 任务#016 S-01 / #017 T-01：自动投影能力判定——固定宿主无每
        # 请求可信结束契约（provider 错误 role=err 经 ERROR 返回、外层
        # 任务取消、后置 hook 停止事件三类路径无结束回调；空结果/正常
        # 流式仅跳过 after，不改变判定），按安全降级条款关闭自动投影
        # 与 AI 图片句柄暴露；判定不可被配置绕过。
        self._auto_projection_enabled, self._auto_projection_reason = \
            auto_projection_support()
        if not self._auto_projection_enabled:
            logger.warning("[角色小衣柜] 自动投影已安全关闭：%s",
                           self._auto_projection_reason)
        await self._service.images.recover_untracked_files()
        logger.info("[角色小衣柜] 服务已启动，数据目录 %s", data_root)

    def _release_record_handles(self, handle_ids: list[str]) -> None:
        """投影记录释放回调（同步）：按记录在案 handle_id 精确清偿
        本插件句柄（不按 turn——并发同 turn 重复 hook 时不得误删新
        hook 刚注册的句柄）。"""
        for hid in handle_ids:
            self._handles.expire_handle(hid)

    @staticmethod
    async def _wait_all(tasks: set) -> None:
        """R-01：等待一组任务真实完成；等待被取消不取消任何任务
        （与 service._wait_all 同协议）。"""
        waiting = list(tasks)
        while waiting:
            _done, pending = await asyncio.wait(waiting)
            waiting = list(pending)

    async def terminate(self) -> None:
        """R-01 停用顺序：停止准入 → 等待在途 hook/工具请求真实完成
        （含最后注入）→ 摘除自己全部 part/记录并清空句柄 → 服务在途/
        后台结算 → DB 最后关闭。

        等待被取消不取消任务：stop 取消/重复 stop 可继续结算。等待
        期间投影对象保持存活——在途 hook 正常完成注入，不再出现
        "最后读取返回后 terminate 提前清空投影"的 None 撞击窗口。
        """
        self._closing = True  # 停止准入：新 hook/工具请求即拒
        await self._wait_all(self._active_requests)
        if self._projection is not None:
            self._projection.expire_turn(None)
            self._projection = None
        self._handles.expire_turn(None)
        if self._service:
            await self._service.stop()
            self._service = None
        logger.info("[角色小衣柜] 服务已停止（数据保留）")

    @contextlib.asynccontextmanager
    async def _plugin_request(self):
        """R-01：整个已接受工具请求的停用所有权。

        _closing 后新请求拒绝（ServiceStopping → 工具层 rejected）；
        在途请求登记为整体任务，terminate 等待其真实完成（含全部
        service 调用与收尾）后才开始清理。
        """
        if self._closing:
            raise ServiceStopping("服务正在停用，新请求已拒绝")
        task = asyncio.current_task()
        self._active_requests.add(task)
        try:
            yield
        finally:
            self._active_requests.discard(task)

    @property
    def service(self) -> WardrobeService:
        if self._service is None:
            raise RuntimeError("插件尚未初始化或已停止")
        return self._service

    # ------------------------------------------------------------------
    # 身份解析（R1：两段式）
    # ------------------------------------------------------------------

    async def _resolve_persona_for_command(self, event: AstrMessageEvent):
        """命令路径复刻宿主 persona 解析链（与 LLM 请求同一来源）。"""
        ctx = self.context
        conv_id = await ctx.conversation_manager.get_curr_conversation_id(
            event.unified_msg_origin)
        conv = None
        if conv_id:
            conv = await ctx.conversation_manager.get_conversation(
                event.unified_msg_origin, conv_id)
        persona_id, persona, _forced, _webchat = (
            await ctx.persona_manager.resolve_selected_persona(
                umo=event.unified_msg_origin,
                conversation_persona_id=conv.persona_id if conv else None,
                platform_name=event.get_platform_name(),
                provider_settings=ctx.get_config(umo=event.unified_msg_origin),
            ))
        return (persona_id,
                persona.get("name") if isinstance(persona, dict) else None)

    def _event_message_id(self, event: AstrMessageEvent) -> str:
        """真实事件消息 ID；缺失时返回空串——不伪造随机 ID（幂等键禁用）。"""
        return str(getattr(event.message_obj, "message_id", "") or "").strip()

    def _request_key(self, identity, action: str) -> str | None:
        """G3：幂等键绑定完整作用域（平台|窗口|脱敏sender|消息|动作）。

        message_id 缺失（平台未提供稳定 ID）时返回 None——不启用幂等，
        也不用随机 UUID 冒充可持久重放（手工/无 ID 策略见 TECHNICAL.md §5）。
        """
        if not identity.message_id:
            return None
        return (f"cmd:{identity.platform_name}|{identity.umo}"
                f"|{identity.sender_ref}|{identity.message_id}|{action}")

    async def _persona_recheck(self, event: AstrMessageEvent):
        """H1：请求级宿主核验回调——重走宿主解析链取当前 persona/管理员态。

        该回调在最终写事务准入后执行（service._host_pre_commit →
        repository._Tx.pre_commit），不再使用可被并发请求覆盖的共享字段。
        """
        persona_id, _display = await self._resolve_persona_for_command(event)
        return persona_id, bool(event.is_admin())

    def _bind_persona_recheck(self, event: AstrMessageEvent):
        return lambda: self._persona_recheck(event)

    async def _resolve_identity(self, event: AstrMessageEvent):
        """只解析最终 persona 与真实身份；不要求角色/授权已存在。"""
        persona_id, persona_display = await self._resolve_persona_for_command(event)
        identity = build_identity(
            umo=event.unified_msg_origin,
            platform_name=event.get_platform_name(),
            sender_id=event.get_sender_id() or "",
            persona_id=persona_id,
            persona_display=persona_display,
            is_host_admin=event.is_admin(),
            message_id=self._event_message_id(event),
            namespace=self._handles.namespace,
        )
        return identity, persona_id, persona_display

    async def _resolve_scope(self, event: AstrMessageEvent, action_nonce: str,
                             minimum_level: str = "reader"):
        """身份 → 角色 → 窗口+逐人授权快照 → 不可伪造 TurnScope。"""
        identity, persona_id, _display = await self._resolve_identity(event)
        role = await self.service.roles.resolve(persona_id)
        snapshot = await self.service.permissions.snapshot(
            role.role_id, identity.umo, identity.sender_ref,
            identity.is_host_admin)
        turn_token = f"{identity.message_id}:{action_nonce}"
        scope = build_scope(
            role_id=role.role_id,
            binding_revision=role.binding_revision,
            grant_revision=snapshot.revision,
            message_id=identity.message_id,
            action_nonce=action_nonce,
        )
        # 补齐复核所需字段（build_scope 只构造基础 token）
        from dataclasses import replace

        scope = replace(scope, umo=identity.umo, sender_ref=identity.sender_ref,
                        auth_generation=snapshot.auth_generation,
                        is_host_admin=identity.is_host_admin,
                        minimum_level=minimum_level)
        return identity, persona_id, scope, turn_token

    def _image_specs_from_event(self, event: AstrMessageEvent,
                                allow_reply: bool) -> list[dict]:
        """从真实消息链提取图片组件规格；引用图仅在 allow_reply 时接受。"""
        specs: list[dict] = []
        for component in event.get_messages():
            if isinstance(component, Image):
                spec = classify_image_component(
                    getattr(component, "file", None),
                    getattr(component, "url", None),
                    getattr(component, "path", None))
                if spec:
                    specs.append({**spec, "from_reply": False})
            elif isinstance(component, Reply):
                chain = getattr(component, "chain", None) or []
                for sub in chain:
                    if isinstance(sub, Image):
                        spec = classify_image_component(
                            getattr(sub, "file", None),
                            getattr(sub, "url", None),
                            getattr(sub, "path", None))
                        if spec:
                            specs.append({**spec, "from_reply": True})
        if not allow_reply:
            specs = [s for s in specs if not s.get("from_reply")]
        return specs

    def _register_turn_handles(self, event: AstrMessageEvent, identity,
                               role_id: str, turn_token: str, allow_reply: bool):
        specs = self._image_specs_from_event(event, allow_reply)
        return self._handles.register_from_components(
            turn_token, identity.umo, identity.sender_ref, role_id,
            identity.sender_ref, identity.platform_name, specs, allow_reply)

    @staticmethod
    def _error_text(exc: WardrobeError) -> str:
        messages = {
            "role_unbound": f"当前会话没有可用的角色绑定：{exc}",
            "binding_exists": "该人格已绑定其他角色；迁移请使用显式重绑定流程。",
            "not_initialized": "当前窗口尚未授权访问衣柜。",
            "permission_denied": f"没有权限：{exc}",
            "stale_context": "请求已过期（授权或角色绑定发生变化），请重新发起。",
            "version_conflict": "当前穿搭刚被其他窗口修改，请重新查看后再试。",
            "manual_lock_active": f"手工穿搭锁生效中：{exc}",
            "quota_exceeded": f"已达到上限：{exc}",
            "image_rejected": f"图片被拒绝：{exc}",
            "handle_invalid": "图片来源无效或已过期。",
            "item_not_found": "服装不存在或已删除。",
            "invalid_input": f"输入不合法：{exc}",
            "idempotency_conflict": "重复请求冲突，已拒绝。",
            "schema_too_new": "数据库版本高于当前插件，请使用对应版本插件。",
            "service_stopping": "衣柜服务正在停用，请稍后再试。",
            "tx_outcome_unknown": (
                "本次操作提交结果暂时无法确认（未返回成功也不代表失败）；"
                "不要重复发起同一操作，请使用 /衣柜 核对 <凭据> 查询结果。"),
        }
        return f"[角色小衣柜] {messages.get(exc.code, str(exc))}"

    @staticmethod
    def _extract_prefix_category(text: str) -> tuple[str | None, str]:
        """从名称前缀识别类别声明（如“连衣裙 蓝裙” → 连衣裙/蓝裙）。"""
        for cat in ("连衣裙", "上装", "下装", "外套", "套装", "配饰"):
            if text.startswith(cat) and len(text) > len(cat):
                return cat, text[len(cat):].strip()
        if text.startswith("鞋") and len(text) > 1:
            return "鞋", text[1:].strip()
        return None, text

    # ------------------------------------------------------------------
    # 命令入口（GreedyStr 尾参：官方契约下普通 str 只收一个 token）
    # ------------------------------------------------------------------

    @filter.command("衣柜", alias={"wardrobe"})
    async def wardrobe_command(self, event: AstrMessageEvent,
                               action: str = "", rest: GreedyStr = GreedyStr):
        """角色小衣柜主命令。用法：/衣柜 初始化|列表|添加|查看|编辑|删除|穿|当前|解锁|授权|撤权|清空

        G1 官方契约（v4.28.0 command.py init_handler_md + validate_and_convert_params）：
        登记值（默认值优先于注解）须 `is GreedyStr` 才贪婪；action 有默认值在前，
        Python 语法要求 rest 也有默认值——因此默认值必须是 GreedyStr 类型本身。
        greedy 分支在缺失检查之前且总是绑定 " ".join(剩余 token)（无尾参时空串），
        该默认值永远不会作为实参出现；isinstance 防御仅覆盖非派发直调。
        """
        rest = "" if isinstance(rest, type) else (rest or "")
        action = (action or "").strip()
        rest = rest.strip()
        handler = self._dispatch_action(action)
        if handler is None:
            known = ("初始化 列表 添加 查看 编辑 删除 穿 当前 风格 自主"
                     " 核对 解锁 授权 撤权 清空")
            yield event.plain_result(
                f"[角色小衣柜] 未知子命令。可用：{known}")
            return
        try:
            async for result in handler(event, rest):
                yield result
        finally:
            # H1：宿主核验回调随请求参数传递（host_verify），不再有共享
            # 字段——读命令完成不影响其他请求的核验器，无需在此清理
            pass

    def _dispatch_action(self, action: str):
        mapping = {
            "初始化": self._cmd_initialize,
            "列表": self._cmd_list,
            "添加": self._cmd_add,
            "查看": self._cmd_view,
            "编辑": self._cmd_edit,
            "删除": self._cmd_delete,
            "穿": self._cmd_wear,
            "当前": self._cmd_current,
            "风格": self._cmd_style,
            "自主": self._cmd_autonomy,
            "核对": self._cmd_verify,
            "解锁": self._cmd_unlock,
            "授权": self._cmd_grant,
            "撤权": self._cmd_revoke,
            "清空": self._cmd_clear,
        }
        return mapping.get(action)

    async def _cmd_style(self, event, rest):
        """`/衣柜 风格`：无参查看（reader）；有参设置（maintainer）。"""
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "style")
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        try:
            if not rest.strip():
                profile = await self.service.style_get(
                    persona_id, scope, identity.sender_ref, identity.umo,
                    identity.is_host_admin)
                lines = ["[角色小衣柜] 当前风格：",
                         f"喜欢标签：{'、'.join(profile['preferred_tags']) or '无'}",
                         f"避开标签：{'、'.join(profile['avoid_tags']) or '无'}",
                         f"场景偏好：{profile['scene_preferences'] or '无'}",
                         f"说明：{profile['note'] or '无'}",
                         f"版本：{profile['revision']}"]
                yield event.plain_result("\n".join(lines))
                return
            fields = parse_style_args(rest)
            if not fields:
                yield event.plain_result(
                    "[角色小衣柜] 风格设置示例：风格 喜欢 连衣裙,优雅"
                    " 避开 运动 场景 日常外出 说明 偏好长裙")
                return
            profile = await self.service.style_set(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, fields,
                host_verify=self._bind_persona_recheck(event))
            yield event.plain_result(
                f"[角色小衣柜] 风格已更新（版本 {profile['revision']}）：\n"
                f"喜欢标签：{'、'.join(profile['preferred_tags']) or '无'}\n"
                f"避开标签：{'、'.join(profile['avoid_tags']) or '无'}\n"
                f"场景偏好：{profile['scene_preferences'] or '无'}\n"
                f"说明：{profile['note'] or '无'}")
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
        finally:
            if turn_token:
                self._handles.expire_turn(turn_token)

    async def _cmd_autonomy(self, event, rest):
        """`/衣柜 自主`：无参查看（reader）；on/off/add/wear 设置（管理者）。"""
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "autonomy")
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        try:
            updates = parse_switch_args(rest)
            if not updates:
                view = await self.service.autonomy_get(
                    persona_id, scope, identity.sender_ref, identity.umo,
                    identity.is_host_admin)
                window_note = ("" if view["allow_ai_mutation"]
                               else "\n当前窗口未允许角色自主变更。")
                # R-03：区分"已保存开关""窗口许可""实际能力"——实际 AI
                # 写入能力取同一受控服务视图（与 cw_get_state/投影同源），
                # 命令层不伪造可用。
                capability = ("" if view.get("tool_write_available", False)
                              else ("\n实际 AI 写入：暂不可用"
                                    f"（{view.get('tool_write_reason', '')}）；"
                                    "查看与手工功能可用。"))
                # 任务#016 S-01：自动投影安全关闭的如实披露。
                projection_note = ("" if view.get("auto_projection_enabled")
                                   else ("\n自动投影：已关闭"
                                         f"（{view.get('auto_projection_reason', '')}）。"))
                yield event.plain_result(
                    "[角色小衣柜] 自主行为状态：\n"
                    f"总开关：{'开' if view['master_enabled'] else '关'}\n"
                    f"自主添衣：{'开' if view['add_enabled'] else '关'}"
                    f"（今日 {view['add_count']}/{view['daily_add_limit']}，"
                    f"冷却剩余 {view['add_cooldown_remaining_s']} 秒）\n"
                    f"自主选穿：{'开' if view['wear_enabled'] else '关'}\n"
                    f"窗口自主写入：{'允许' if view['allow_ai_mutation'] else '禁止'}"
                    f"{window_note}{capability}{projection_note}")
                return
            view = await self.service.autonomy_set(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, updates,
                host_verify=self._bind_persona_recheck(event))
            parts = []
            if "master" in updates:
                parts.append(f"总开关→{'开' if updates['master'] else '关'}")
            if "add" in updates:
                parts.append(f"自主添衣→{'开' if updates['add'] else '关'}")
            if "wear" in updates:
                parts.append(f"自主选穿→{'开' if updates['wear'] else '关'}")
            # R-03：设置回复同样如实披露实际能力（同一受控视图字段）；
            # 总 on 只是保存开关，不宣称启用宿主缺失的能力。
            capability_note = ("" if view.get("tool_write_available", False)
                               else ("实际 AI 写入仍暂不可用"
                                     f"（{view.get('tool_write_reason', '')}）。"))
            projection_note = ("" if view.get("auto_projection_enabled")
                               else (f"自动投影已关闭"
                                     f"（{view.get('auto_projection_reason', '')}）。"))
            yield event.plain_result(
                "[角色小衣柜] 已更新："
                + "；".join(parts)
                + f"（开关版本 {view['revision']}）。"
                + capability_note
                + projection_note
                + "开关已保存；库存/风格/当前穿搭保留，"
                "手工功能不受影响。")
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
        finally:
            if turn_token:
                self._handles.expire_turn(turn_token)

    # -- 各子命令实现（全部转交真实 persona + TurnScope，R1） ------------

    async def _cmd_initialize(self, event, rest):
        try:
            identity, persona_id, persona_display = await self._resolve_identity(event)
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        try:
            existing = await self.service.roles.resolve(persona_id)
        except WardrobeError:
            existing = None
        if existing is not None:
            snapshot = None
            try:
                snapshot = await self.service.permissions.snapshot(
                    existing.role_id, identity.umo, identity.sender_ref,
                    identity.is_host_admin)
            except WardrobeError:
                snapshot = None
            note = ("当前窗口已授权。" if snapshot is not None
                    else "当前窗口尚未授权，请在本窗口执行 /衣柜 授权 窗口 或由管理者处理。")
            yield event.plain_result(
                f"[角色小衣柜] 该人格已初始化为角色「{existing.display_name}」。{note}")
            return
        if not identity.is_host_admin:
            yield event.plain_result(
                "[角色小衣柜] 首次初始化需要宿主管理员执行；普通成员请等待管理者开通。")
            return
        try:
            display = persona_display or existing_display_fallback(persona_id)
            result = await self.service.initialize(
                persona_id, display, identity.sender_ref, identity.umo,
                host_verify=self._bind_persona_recheck(event),
                is_host_admin=identity.is_host_admin)
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        yield event.plain_result(
            f"[角色小衣柜] 已为角色「{result['display_name']}」建立稳定衣柜；"
            "同一人格的其他窗口执行 /衣柜 授权 窗口 后共享同一份衣柜。")

    def _unknown_result_text(self, exc: TxOutcomeUnknown) -> str:
        """I6：结果未知回复——给出可用脱敏凭据与准确核对指引。"""
        text = self._error_text(exc)
        credential = getattr(exc, "credential", None)
        if credential:
            text += (f"\n本次核对凭据：{credential}\n"
                     f"发送 /衣柜 核对 {credential} 查询该次操作的真实结果"
                     "（只读核对，不会重复执行、不会重复计费）。")
        else:
            text += "\n暂时无法给出核对凭据；请稍后重试或联系管理者核查。"
        return text

    async def _cmd_verify(self, event, rest):
        """I6：公开原操作只读核对——凭据定位原请求，返回原结算状态/原结果。

        不再次执行 add/wear、不占用变更额度；跨角色/跨发起者/未授权窗口
        或凭据无效时统一拒绝，不回显原始消息 ID/路径/身份。重复核对幂等。
        """
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "verify")
            token = rest.strip()
            if not token:
                yield event.plain_result(
                    "[角色小衣柜] 用法：/衣柜 核对 <核对凭据>"
                    "（只读查询，不会重复执行任何操作）")
                return
            try:
                outcome = await self.service.verify_settlement(
                    persona_id, scope, identity.sender_ref, identity.umo,
                    identity.is_host_admin, token)
            except WardrobeError as exc:
                yield event.plain_result(self._error_text(exc))
                return
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        state = outcome.get("state")
        action = outcome.get("action") or ""
        action_name = "添加服装" if action == "add_item" else "换装"
        if state == "committed":
            result = outcome.get("result") or {}
            if action == "add_item":
                summary = (f"「{result.get('name', '')}」已入库"
                           f"（ID: {result.get('item_id', '')}，"
                           f"含 {result.get('asset_count', 0)} 张已验证图片）")
            else:
                summary = (f"已换好（版本 {result.get('revision', '?')}，"
                           f"共 {len(result.get('item_ids', []))} 件）")
            yield event.plain_result(
                f"[角色小衣柜] 核对结果：原{action_name}操作已提交。{summary}")
            return
        if state == "not_committed":
            yield event.plain_result(
                f"[角色小衣柜] 核对结果：原{action_name}操作确认未提交，"
                "衣柜未变更、未计费；可按需重新发起。")
            return
        yield event.plain_result(
            "[角色小衣柜] 核对结果：该次操作仍在核对中（结果未知），"
            "请稍后再次核对；请勿重复发起同一操作。")

    async def _cmd_list(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "list")
            page = 1
            category = None
            tokens = rest.split() if rest else []
            for token in tokens:
                if token.isdigit():
                    page = int(token)
                else:
                    category = token
            result = await self.service.list_items(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, page=page, category=category)
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        if not result["items"]:
            yield event.plain_result(
                "[角色小衣柜] 衣柜还是空的。用 /衣柜 添加 来添第一件衣服吧。")
            return
        lines = [f"[角色小衣柜] 第 {result['page']}/{result['pages']} 页，"
                 f"共 {result['total']} 件："]
        for item in result["items"]:
            mark = "[图]" if item["has_image"] else "    "
            own = "（我添加）" if item["is_own"] else ""
            lines.append(f"{mark} {item['item_id'][:14]}… {item['name']}"
                         f"（{item['category']}）{own}")
        yield event.plain_result("\n".join(lines))

    async def _cmd_add(self, event, rest):
        turn_token = None
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "add", minimum_level="contributor")
            allow_reply = bool(self._config.allow_reply_image)
            handles = self._register_turn_handles(
                event, identity, scope.role_id, turn_token, allow_reply)
            if not rest:
                yield event.plain_result(
                    "[角色小衣柜] 用法：/衣柜 添加 [类别] <名称> [描述]，可附图片；"
                    "类别可省略（如“添加 连衣裙 蓝裙 很好看”）")
                return
            # H9：两种类别形态（首 token 显式类别 / 名称前缀类别）都保留
            # 名称之后**全部剩余尾参**作为描述——GreedyStr 已保证完整，
            # 不再因固定 maxsplit 丢掉第 3 个及之后的 token
            tokens = rest.split()
            category: str | None = None
            name_rest = rest
            if tokens[0] in _ALL_CATEGORY_TOKENS:
                category = tokens[0]
                name_rest = " ".join(tokens[1:])
            name_tokens = name_rest.split(maxsplit=1)
            if not name_tokens:
                yield event.plain_result(
                    "[角色小衣柜] 缺少名称：/衣柜 添加 <类别> <名称> [描述]")
                return
            name = name_tokens[0]
            description = name_tokens[1].strip() if len(name_tokens) > 1 else ""
            if category is None:
                prefix_cat, name_clean = self._extract_prefix_category(name)
                if prefix_cat:
                    category = prefix_cat
                    name = name_clean
            category = category or "配饰"
            result = await self.service.add_item(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, name=name, description=description,
                category=category, tags=[], layerable=False,
                source_kind="user", handles=handles,
                request_key=self._request_key(identity, "add"),
                host_verify=self._bind_persona_recheck(event))
        except TxOutcomeUnknown as exc:
            yield event.plain_result(self._unknown_result_text(exc))
            return
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        finally:
            if turn_token:
                self._handles.expire_turn(turn_token)
        image_note = (f"（含 {result['asset_count']} 张已验证图片）"
                      if result["asset_count"] else "")
        replay = "（重复请求，已按原结果返回）" if result.get("idempotent_replay") else ""
        yield event.plain_result(
            f"[角色小衣柜] 已收进衣柜：{result['name']}"
            f"（{result['category']}）{image_note}{replay}\nID: {result['item_id']}")

    async def _cmd_view(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "view")
            tokens = rest.split()
            if not tokens:
                yield event.plain_result("[角色小衣柜] 用法：/衣柜 查看 <服装ID>")
                return
            detail = await self.service.view_item(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, tokens[0])
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        lines = [
            f"[角色小衣柜] {detail['name']}（{detail['category']}）",
            f"ID: {detail['item_id']}",
        ]
        if detail["description"]:
            lines.append(f"描述：{detail['description']}")
        if detail["tags"]:
            lines.append(f"标签：{'、'.join(detail['tags'])}")
        chain: list = [Plain(text="\n".join(lines) + "\n")]
        for asset in detail["assets"]:
            try:
                path = await self.service.item_image_path(
                    persona_id, scope, identity.sender_ref, identity.umo,
                    identity.is_host_admin, asset["asset_id"])
                chain.append(Image.fromFileSystem(str(path)))
            except WardrobeError:
                chain.append(Plain(text="（图片加载失败）\n"))
        caption_note = ""
        if detail["assets"] and detail["assets"][0]["caption_state"] == "none":
            caption_note = "（图片未自动标注）"
        chain.append(Plain(text=caption_note or ""))
        yield event.chain_result(chain)

    async def _cmd_edit(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "edit", minimum_level="contributor")
            tokens = rest.split(maxsplit=2)
            if len(tokens) < 2:
                yield event.plain_result(
                    "[角色小衣柜] 用法：/衣柜 编辑 <服装ID> <名称|描述|标签|类别|可叠穿> <内容>")
                return
            item_id, field_name = tokens[0], tokens[1]
            value = tokens[2] if len(tokens) > 2 else ""
            mapped = _EDIT_FIELD_MAP.get(field_name)
            if mapped is None:
                yield event.plain_result(
                    f"[角色小衣柜] 未知字段「{field_name}」；"
                    f"支持：{'、'.join(_EDIT_FIELD_MAP)}")
                return
            if mapped == "layerable":
                # H9：有限拼写表解析为真正布尔；空/未知/非法值拒绝且无副作用
                try:
                    fields = {mapped: parse_layerable_value(value)}
                except WardrobeError as exc:
                    yield event.plain_result(self._error_text(exc))
                    return
            else:
                fields = {mapped: value}
            result = await self.service.edit_item(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, item_id, fields,
                host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        yield event.plain_result(
            f"[角色小衣柜] 已更新（revision {result['revision']}）")

    async def _cmd_delete(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "delete", minimum_level="contributor")
            tokens = rest.split()
            if not tokens:
                yield event.plain_result(
                    "[角色小衣柜] 用法：/衣柜 删除 <服装ID> [force]")
                return
            item_id = tokens[0]
            force = len(tokens) > 1 and tokens[1] == "force"
            result = await self.service.delete_item(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, item_id, force=force,
                host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        yield event.plain_result(f"[角色小衣柜] 已删除 {result['deleted']}")

    async def _cmd_wear(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "wear", minimum_level="contributor")
            item_ids = rest.replace("，", " ").replace(",", " ").split()
            if not item_ids:
                yield event.plain_result(
                    "[角色小衣柜] 用法：/衣柜 穿 <服装ID…>（可先 /衣柜 当前 查看版本）")
                return
            outfit = await self.service.current_outfit(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin)
            result = await self.service.wear(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, item_ids,
                expected_revision=outfit["revision"], source_kind="user",
                request_key=self._request_key(identity, "wear"),
                host_verify=self._bind_persona_recheck(event))
        except TxOutcomeUnknown as exc:
            yield event.plain_result(self._unknown_result_text(exc))
            return
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        replay = "（重复请求，已按原结果返回）" if result.get("idempotent_replay") else ""
        yield event.plain_result(
            f"[角色小衣柜] 已换好（version {result['revision']}，"
            f"共 {len(result['item_ids'])} 件）{replay}")

    async def _cmd_current(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "current")
            outfit = await self.service.current_outfit(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin)
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        if not outfit["items"]:
            yield event.plain_result(
                "[角色小衣柜] 当前没有穿着搭配（version 0）。")
            return
        lines = [f"[角色小衣柜] 当前穿搭（version {outfit['revision']}）："]
        for entry in outfit["items"]:
            lines.append(f"- {entry['name']}（{entry['category']}）")
        yield event.plain_result("\n".join(lines))

    async def _cmd_unlock(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "unlock", minimum_level="maintainer")
            await self.service.unlock(persona_id, scope, identity.sender_ref,
                                      identity.umo, identity.is_host_admin,
                                      host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        yield event.plain_result("[角色小衣柜] 手工穿搭锁已清除。")

    async def _cmd_grant(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "grant", minimum_level="maintainer")
            tokens = rest.split()
            if not tokens:
                yield event.plain_result(
                    "[角色小衣柜] 用法：/衣柜 授权 窗口 [目标UMO]｜"
                    "授权 读取|贡献|管理 [成员senderID]")
                return
            kind = tokens[0]
            target = tokens[1] if len(tokens) > 1 else None
            if kind == "窗口":
                target_umo = target or identity.umo
                await self.service.grant_window(
                    persona_id, scope, identity.sender_ref, identity.umo,
                    identity.is_host_admin, target_umo,
                    host_verify=self._bind_persona_recheck(event))
                yield event.plain_result("[角色小衣柜] 窗口已启用共享（成员默认读取）。")
                return
            if kind not in _GRANT_LEVELS:
                yield event.plain_result(
                    f"[角色小衣柜] 未知授权动作「{kind}」；"
                    "支持：窗口、读取、贡献、管理。")
                return
            if not target:
                yield event.plain_result(
                    "[角色小衣柜] 逐人授权需要成员 senderID（与当前窗口同平台）。")
                return
            target_ref = masked_actor_ref(
                self._handles.namespace, target, identity.platform_name)
            await self.service.grant_sender(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, target_ref, _GRANT_LEVELS[kind],
                host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        level_name = {"读取": "读取", "贡献": "贡献", "管理": "管理"}.get(kind, kind)
        yield event.plain_result(
            f"[角色小衣柜] 已把该成员调整为 {level_name} 级别。")

    async def _cmd_revoke(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "revoke", minimum_level="maintainer")
            tokens = rest.split()
            kind = tokens[0] if tokens else "成员"
            target = tokens[1] if len(tokens) > 1 else None
            if kind == "窗口":
                target_umo = target
                if not target_umo:
                    yield event.plain_result(
                        "[角色小衣柜] 撤销窗口需要目标 UMO：/衣柜 撤权 窗口 <UMO>")
                    return
                await self.service.revoke(
                    persona_id, scope, identity.sender_ref, identity.umo,
                    identity.is_host_admin, target_umo,
                    host_verify=self._bind_persona_recheck(event))
                yield event.plain_result("[角色小衣柜] 已撤销该窗口授权。")
                return
            if not target:
                yield event.plain_result(
                    "[角色小衣柜] 撤销成员权限需要 senderID：/衣柜 撤权 成员 <senderID>")
                return
            target_ref = masked_actor_ref(
                self._handles.namespace, target, identity.platform_name)
            await self.service.grant_sender(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, target_ref, None,
                host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        yield event.plain_result("[角色小衣柜] 已把该成员降回读取级别。")

    async def _cmd_clear(self, event, rest):
        try:
            identity, persona_id, scope, turn_token = await self._resolve_scope(
                event, "clear", minimum_level="maintainer")
            if rest.strip() != "confirm":
                yield event.plain_result(
                    "[角色小衣柜] 清空会删除该角色全部衣柜数据且不可恢复。"
                    "确认请执行：/衣柜 清空 confirm")
                return
            result = await self.service.clear_role(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, "confirm",
                host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            yield event.plain_result(self._error_text(exc))
            return
        yield event.plain_result(
            f"[角色小衣柜] 已清空该角色衣柜（清理图片 {result['assets_removed']} 张）；"
            "自主行为已关闭。")

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # M2：模型工具与临时投影（Q-01：官方加载器 star_manager L1253-1298
    # 以 functools.partial(raw_handler, plugin) 绑定实例后按
    # handler(event, **params) 派发——第一形参是插件实例 self，第二才
    # 是真实 AstrMessageEvent；不使用模块级单例，双实例天然隔离。模型
    # 参数仅限 docstring 白名单字段并原样传给核心层做严格类型验证
    # （Q-08）；身份/角色/权限/UMO 全部服务端从 event 派生。）
    # ------------------------------------------------------------------

    async def _tool_context(self, event, minimum_level: str = "reader"):
        """工具路径身份解析（复用 M1 链；未初始化/未授权抛 WardrobeError）。"""
        return await self._resolve_scope(event, "tool", minimum_level)

    def _turn_identity(self, identity) -> str:
        """统一可信 turn 标识（句柄/投影/收尾/预算共用；与 Q-04 同源）。"""
        Repository = _wardrobe_pkg.repository.Repository

        return Repository.turn_identity(
            identity.platform_name, identity.umo, identity.sender_ref,
            identity.message_id)

    @filter.llm_tool(name="cw_list_items")
    async def cw_list_items(self, event, query: str = "",
                            category: str = "", page: int = 1) -> str:
        """查看角色衣柜中的服装条目（有限检索与分页，不返回路径或链接）。

        Args:
            query(string): 可选检索词（匹配名称/描述/标签），可留空。
            category(string): 可选类别，仅限：上装、下装、连衣裙、外套、鞋、配饰、套装。可留空。
            page(number): 页码，默认 1。
        """
        # R-01：整个工具请求纳入停用所有权（_closing 后 ServiceStopping
        # → rejected），并登记在途任务供 terminate 等待。
        try:
            async with self._plugin_request():
                identity, persona_id, scope, _turn = await self._tool_context(
                    event, "reader")
                # Q-08：原样透传，严格类型验证在核心层（任何副作用之前）
                return await tool_list_items(
                    self.service, persona_id, scope, identity.sender_ref,
                    identity.umo, identity.is_host_admin,
                    query=query, category=category, page=page)
        except WardrobeError as exc:
            return _tool_rejected(exc)

    @filter.llm_tool(name="cw_get_state")
    async def cw_get_state(self, event) -> str:
        """查看角色当前穿搭、风格偏好、自主开关与手工锁状态（只读）。"""
        try:
            async with self._plugin_request():
                identity, persona_id, scope, _turn = await self._tool_context(
                    event, "reader")
                return await tool_get_state(
                    self.service, persona_id, scope, identity.sender_ref,
                    identity.umo, identity.is_host_admin)
        except WardrobeError as exc:
            return _tool_rejected(exc)

    @filter.llm_tool(name="cw_add_item")
    async def cw_add_item(self, event, name: str = "", category: str = "",
                          description: str = "", tags: str = "",
                          source_handle: str = "") -> str:
        """把一件新衣服收进角色衣柜；没有可信图片句柄时创建无图文字条目。

        Args:
            name(string): 服装名称。
            category(string): 类别，仅限：上装、下装、连衣裙、外套、鞋、配饰、套装。
            description(string): 服装描述，可留空。
            tags(string): 逗号分隔的标签，可留空。
            source_handle(string): 本轮消息提供的可信图片句柄 ID，可留空表示无图。
        """
        try:
            async with self._plugin_request():
                identity, persona_id, scope, _turn = \
                    await self._tool_context(event, "contributor")
                handle = None
                token = source_handle if isinstance(source_handle, str) else ""
                if token and not token.isspace():
                    try:
                        # 句柄严格入口：绑定 turn/UMO/actor/role 逐一复核；非法
                        # 句柄拒绝（不静默当无图）
                        handle = self._handles.get_verified(
                            token, turn_token=self._turn_identity(identity),
                            umo=identity.umo, sender_ref=identity.sender_ref,
                            role_id=scope.role_id)
                    except WardrobeError as exc:
                        return _tool_rejected(exc)
                # Q-03：该固定宿主不向插件提供当前调用 tool_call_id——不传
                # call_id，核心层实际 rejected（fail-closed，零副作用）。
                return await tool_add_item(
                    self.service, persona_key=persona_id, scope=scope,
                    actor_ref=identity.sender_ref, umo=identity.umo,
                    is_host_admin=identity.is_host_admin,
                    platform=identity.platform_name,
                    turn_identity=self._turn_identity(identity),
                    name=name, category=category, description=description,
                    tags=tags, handle=handle, call_id="",
                    host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            return _tool_rejected(exc)

    @filter.llm_tool(name="cw_wear_items")
    async def cw_wear_items(self, event, item_ids: list[str] | None = None,
                            expected_revision: int = -1) -> str:
        """让角色穿上衣柜中已有的服装（需提供当前穿搭版本号）。

        Args:
            item_ids(array[string]): 要穿着的服装 ID 列表（本角色衣柜内）。
            expected_revision(number): 当前穿搭版本号，用于并发保护。
        """
        try:
            async with self._plugin_request():
                identity, persona_id, scope, _turn = \
                    await self._tool_context(event, "contributor")
                # Q-03：同 add——无可信调用 ID，AI 选穿在正式入口实际拒绝。
                return await tool_wear_items(
                    self.service, persona_key=persona_id, scope=scope,
                    actor_ref=identity.sender_ref, umo=identity.umo,
                    is_host_admin=identity.is_host_admin,
                    platform=identity.platform_name,
                    turn_identity=self._turn_identity(identity),
                    item_ids=item_ids, expected_revision=expected_revision,
                    call_id="", host_verify=self._bind_persona_recheck(event))
        except WardrobeError as exc:
            return _tool_rejected(exc)

    # ------------------------------------------------------------------
    # M2：临时投影（P-05/Q-05/Q-06）与 turn 收尾
    # ------------------------------------------------------------------

    @filter.on_llm_request()
    async def wardrobe_on_llm_request(self, event, req) -> None:
        """把有限衣柜状态与可信句柄作为本轮临时部件追加给模型。

        任务#016 S-01 / #017 T-01：本宿主经 `auto_projection_support()`
        判定**无每请求可信结束契约**——三类路径缺可信 on_llm_response
        覆盖：官方 provider 错误 role=err→step ERROR return、外层
        Task.cancel、CW 后置 hook 停止（reset 前 return）；正常完成/
        显式 request_stop/普通 Exception 补派发可达 on_llm_response
        （空回复/正常流式也在正常完成路径上）；空结果/正常流式跳过
        after 只说明无 after，单独不证明没有 on_llm_response。三类缺
        覆盖足以证明不能自动投影——按安全降级条款**关闭自动投影与
        AI 图片句柄暴露**：本 hook 在任何登记/句柄注册/服务读取/注
        入之前直接返回，实际零 part/零 handle/零 record，不进
        _active_requests（无自有在途责任）。显式查询/手工/风格/已保
        存开关继续；手工添衣图片链（M1 `_register_turn_handles`）不
        受影响；关闭不由配置绕过，也不宣称聊天自主产品完成。

        门控分支（`_auto_projection_enabled=True` 的已证明合约宿主）
        保留：停止准入 → 在途登记 → 身份解析 → 注册本轮句柄 → 五次
        服务读取 → 构造并注入投影；finally 对注册句柄后任何异常/取消
        按本请求 handle_id 精确清偿并如实传播。记录由 on_llm_response
        （真实结束信号）/after/TTL/停用清偿，无跨请求清扫。
        """
        if not self._auto_projection_enabled:
            return
        if self._projection is None or self._closing:
            return
        if getattr(req, "extra_user_content_parts", None) is None:
            # S-01 安全降级：宿主缺投影能力时不注入、不暴露句柄、
            # 无短期遗留（如实限制；显式查询与手工功能继续）
            return
        task = asyncio.current_task()
        self._active_requests.add(task)
        created_handles: list[str] = []
        record = None
        try:
            try:
                identity, persona_id, scope, _turn = await self._tool_context(
                    event, "reader")
            except WardrobeError:
                return  # 未初始化/未授权：不投影私有库存
            turn_identity = self._turn_identity(identity)
            handles = self._register_projection_handles(
                event, identity, scope, turn_identity)
            created_handles = [h.handle_id for h in handles]
            service = self.service
            style = await service.style_get(persona_id, scope,
                                            identity.sender_ref, identity.umo,
                                            identity.is_host_admin)
            outfit = await service.current_outfit(persona_id, scope,
                                                  identity.sender_ref,
                                                  identity.umo,
                                                  identity.is_host_admin)
            autonomy_view = await service.autonomy_get(persona_id, scope,
                                                       identity.sender_ref,
                                                       identity.umo,
                                                       identity.is_host_admin)
            candidates = await service.style_candidates(
                persona_id, scope, identity.sender_ref, identity.umo,
                identity.is_host_admin, query="", page=1)
            items_page = await service.list_items(persona_id, scope,
                                                  identity.sender_ref,
                                                  identity.umo,
                                                  identity.is_host_admin,
                                                  page=1)
            text = self._projection.build_text(
                style=style, outfit=outfit, autonomy_view=autonomy_view,
                candidates=candidates, items_page=items_page,
                handles=[h.handle_id for h in handles])
            self._projection.inject(
                req, text, created_handles, turn_identity)
            record = True  # 注入完成：记录持久到结束信号（on_llm_response/after/TTL/停用）
        finally:
            self._active_requests.discard(task)
            if record is None and created_handles:
                # 失败/取消（注入前）：本请求已注册句柄即时清偿——
                # 不留可达责任悬空，也不触碰其他请求的句柄/记录
                for hid in created_handles:
                    self._handles.expire_handle(hid)

    def _register_projection_handles(self, event, identity, scope,
                                     turn_identity: str):
        """注册本轮可信句柄（Q-06 turn 总量硬限额）。

        S-01：不做跨请求清扫——同 turn 旧句柄先撤（重复 hook 不累加，
        仅本 turn）；投影只列 ID 不含 URL/路径；TTL 惰性清扫在注册/
        取用内执行；注册后按 handle_max 截断。"""
        self._handles.expire_turn(turn_identity)
        specs = self._image_specs_from_event(event,
                                             self._config.allow_reply_image)
        created = self._handles.register_from_components(
            turn_token=turn_identity, umo=identity.umo,
            sender_ref=identity.sender_ref, role_id=scope.role_id,
            sender_id=event.get_sender_id() or "",
            platform=identity.platform_name, image_specs=specs,
            allow_reply=self._config.allow_reply_image)
        limit = self._config.autonomy_handle_max
        if len(created) > limit:
            for extra in created[limit:]:
                self._handles.expire_handle(extra.handle_id)
            created = created[:limit]
        return created

    @filter.on_llm_response()
    async def wardrobe_on_llm_response(self, event, resp) -> None:
        """结束信号清偿钩子（S-01）。

        官方可达链（v4.28.0/a4121464）：正常完成（含空回复/正常流式，
        `_complete_with_assistant_response` L201 → on_agent_done）、
        用户显式 request_stop 中断（L1516）及普通 Exception 逃逸
        run_agent 时的补派发（astr_agent_run_util L317-352）——三类
        路径派发 OnLLMResponseEvent（context_utils L95-101 顺序派发，
        钩子异常被宿主吞掉不阻断）。**缺可信覆盖的三类路径**：provider
        错误 role=err、外层 Task.cancel、CW 后置 hook 停止——这正是
        本宿主自动投影被安全关闭的理由。**after 与 response 是两个回
        调**：空结果/正常流式跳过 after_message_sent 只说明无 after，
        单独不证明没有 on_llm_response（任务#017 T-01 纠正）。本钩子
        在关闭分支下无自有记录可清（幂等 no-op 兜底）；门控分支启用
        时按本 turn 即时清偿句柄与投影（其他 turn 一律不动）。身份解
        析失败时无法定位本 turn，由 TTL/停用兜底。
        """
        if self._projection is None and not self._handles._handles:
            return
        try:
            identity, _persona, _display = await self._resolve_identity(
                event)
        except WardrobeError:
            return  # 无法定位本 turn：TTL/停用兜底
        turn_identity = self._turn_identity(identity)
        self._handles.expire_turn(turn_identity)
        if self._projection is not None:
            self._projection.expire_turn(turn_identity)

    @filter.after_message_sent()
    async def wardrobe_after_message_sent(self, event) -> None:
        """消息收尾（官方 respond/stage 空结果/流式路径可跳过——剩余
        责任由 on_llm_response、TTL 与卸载覆盖）：按统一 turn 标识清
        理本轮句柄与投影注入记录（仅本插件对象）。关闭分支下无记录
        （幂等 no-op 兜底）。"""
        identity, _persona, _display = await self._resolve_identity(event)
        turn_identity = self._turn_identity(identity)
        self._handles.expire_turn(turn_identity)
        if self._projection is not None:
            self._projection.expire_turn(turn_identity)


def _tool_rejected(exc: WardrobeError) -> str:
    import json as _json

    return _json.dumps({"state": "rejected", "reason": str(exc)},
                       ensure_ascii=False, sort_keys=True)
def existing_display_fallback(persona_id: str | None) -> str:
    """无显示名时的受控回退（不再引用未定义变量，R1）。"""
    key = (persona_id or "").strip()
    return key[:32] if key else "未命名角色"
