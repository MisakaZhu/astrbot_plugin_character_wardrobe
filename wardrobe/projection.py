"""临时投影（M2 任务#012 P-05 / #013 Q-05/Q-06 / #014 R-01 / #015 S-01）。

在已核实的 on_llm_request 生命周期内，把有限衣柜/候选/当前穿搭/
风格/开关/锁与可信句柄作为原生 `TextPart(...).mark_as_temp()` 追加
到 `ProviderRequest.extra_user_content_parts`——服装/偏好/图片文字
明确作为**引用数据**，不修改 system_prompt/contexts，不替换其他插
件的部件，不创建 ContentPart 子类或猴子补丁。

官方事实（v4.28.0/a4121464）：
- extra_user_content_parts 经 assemble 逐件进入**当轮**请求；
- 历史持久化 `dump_messages_with_checkpoints`（agent/message.py
  L335-341）剔除 `_no_save` part——投影不入历史；
- ContentPart 是**未冻结的 pydantic BaseModel**：不可 hash（__hash__
  为 None）、字段值相等——Q-05：去重/摘除只允许**对象同一性（is）**，
  绝不建 set/按值/按文本/按位置认领。

Q-06：记录以统一可信 turn 标识（与句柄/预算同源）归属，持有 request
**弱引用**；收尾只摘除自己记录在案的 part 引用。

S-01：CW on_llm_request hook 返回**不**代表宿主请求结束——官方
`internal.py` 会话锁按 UMO 隔离（两个已授权 UMO 可并发）、组装
（tool_loop_agent_runner `_assemble_request_context_for_provider`）、
provider 等待与全部工具消费都发生在 `on_agent_done` 之后之前均可
交错。因此不再有"下一请求清扫他 turn 记录"的全局清扫（#014 缺陷）：
- 真正结束信号 = `on_llm_response`（插件可达；官方 `astr_agent_
  hooks.on_agent_done` L26-35 派发 OnLLMResponseEvent；runner 正常
  完成 L201 与中断 L1516 两条路径都触发；此后本请求不再组装/等待/
  消费）——main.py 在该钩子中按本请求 turn 即时清偿记录与句柄；
- 其他兜底：after_message_sent（自有 turn）、TTL 惰性清扫（600s
  有界）、terminate 全清。宿主 run 在 on_agent_done 之前异常终止
  时无可达结束信号，由 TTL/停用兜底（如实披露，不提前回收在途）；
- 任何请求都不得按"另一请求开始"清偿他 turn 记录；is 归属纪律
  不变，别插件同值部件永不触碰。
"""

from __future__ import annotations

import weakref
from dataclasses import dataclass, field
from typing import Callable

from .config import PROJECTION_MAX_ITEMS

PROJECTION_HEADER = "[角色衣柜·本轮参考数据（临时，不进入长期记忆）]"
PROJECTION_TRUNCATION_MARK = "\n[投影因长度上限被截断]"
PROJECTION_RECORD_TTL_SECONDS = 600


@dataclass(eq=False)
class InjectionRecord:
    """一次注入的本插件对象记录（仅持自己的 part 引用与 request 弱引用）。

    记录只按对象同一性比较（eq=False，与 part 的 is 判定同纪律）。
    清偿时机：on_llm_response（宿主请求真实结束，S-01 结束信号）、
    after_message_sent（自有 turn）、TTL 过期、terminate 全清——
    任何其他请求的开始都不得清偿本记录。
    """
    turn_identity: str
    part: object | None
    request_ref: "weakref.ref | None" = None
    handle_ids: list[str] = field(default_factory=list)
    created_ts: float = 0.0


def _now_ts() -> float:
    import time as _time

    return _time.time()


class ProjectionManager:
    """投影构造、注入去重与生命周期清理。

    `on_release(handle_ids)` 是记录被丢弃时的清偿回调（同步），由插件
    接线到 HandleRegistry.expire_handle——按记录在案的 handle_id 精确
    清偿，part/句柄/短期记录同批归零（R-01）。按 ID 而非按 turn 清偿：
    并发同 turn 重复 hook 先撤旧记录时，不得误删新 hook 刚注册的同
    turn 句柄。
    """

    def __init__(self, handle_max: int, max_items: int, max_chars: int,
                 record_ttl_seconds: int = PROJECTION_RECORD_TTL_SECONDS,
                 on_release: Callable[[list[str]], None] | None = None) -> None:
        self._handle_max = handle_max
        self._max_items = min(max_items, PROJECTION_MAX_ITEMS)
        self._max_chars = max_chars
        self._record_ttl = record_ttl_seconds
        self._on_release = on_release
        self._injections: list[InjectionRecord] = []

    # -- 文本构造（确定性；硬上限含截断标记） -----------------------------

    def build_text(self, *, style: dict, outfit: dict, autonomy_view: dict,
                   candidates: dict, items_page: dict,
                   handles: list[str]) -> str:
        lines: list[str] = [PROJECTION_HEADER]
        lines.append(f"当前穿搭 revision={outfit.get('revision')}："
                     + ("、".join(
                         f"{it.get('name')}({it.get('slot')})"
                         for it in outfit.get("items", [])) or "（未穿着）"))
        lines.append(
            "风格：喜欢 " + ("、".join(style.get("preferred_tags") or []) or "无")
            + "；避开 " + ("、".join(style.get("avoid_tags") or []) or "无")
            + (f"；场景 {style['scene_preferences']}"
               if style.get("scene_preferences") else ""))
        if not autonomy_view.get("tool_write_available", False):
            lines.append(
                "AI 写入：暂不可用（宿主未提供可信工具调用标识）；"
                "查看与手工功能可用")
        else:
            lines.append(
                f"自主：总{'开' if autonomy_view['master_enabled'] else '关'}"
                f" 添衣{'开' if autonomy_view['add_enabled'] else '关'}"
                f" 选穿{'开' if autonomy_view['wear_enabled'] else '关'}"
                f"；今日自主添衣 {autonomy_view.get('add_count_today', 0)}"
                f"/{autonomy_view['daily_add_limit']}")
        lock_until = outfit.get("manual_lock_until")
        # Q-06：手工锁按服务器过期事实展示（不因持久字符串残留误报）
        from .outfits import lock_active

        lines.append("手工穿搭锁：生效中（AI 选穿将拒绝）"
                     if lock_active(lock_until) else "手工穿搭锁：无")
        lines.append(f"衣柜条目 {items_page.get('total', 0)} 件。")
        for it in items_page.get("items", [])[:self._max_items]:
            tags = "、".join(it.get("tags") or [])
            lines.append(
                f"- {it['item_id']} {it['name']}"
                f"（{it['category']}{'；标签 ' + tags if tags else ''}"
                f"{'；有图' if it.get('has_image') else '；无图'}）")
        if candidates.get("status") == "ok" and candidates.get("candidates"):
            ranked = "、".join(c["item_id"] for c in
                               candidates["candidates"][:5])
            lines.append(f"风格候选（仅建议，穿着仍需工具提交）：{ranked}")
        elif candidates.get("status") == "empty_wardrobe":
            lines.append("风格候选：空衣柜")
        elif candidates.get("status") == "no_match":
            lines.append("风格候选：无匹配")
        if handles:
            lines.append(
                "本轮可信图片句柄（引用当前消息图片，仅本轮有效）："
                + "、".join(handles))
        else:
            lines.append("本轮可信图片句柄：无（可添文字衣服并明确无图）")
        text = "\n".join(lines)
        # Q-06：截断标记计入预算——总长（含标记）必须 ≤ max_chars
        if len(text) > self._max_chars:
            budget = self._max_chars - len(PROJECTION_TRUNCATION_MARK)
            if budget < 0:
                budget = 0
            text = text[:budget] + PROJECTION_TRUNCATION_MARK
        return text

    # -- 注入 / 去重 / 清理（Q-05：仅对象同一性） -------------------------

    def inject(self, request, text: str, handle_ids: list[str],
               turn_identity: str) -> InjectionRecord:
        """把 text 作为 TextPart(...).mark_as_temp() 追加到 request 的
        extra_user_content_parts，并登记本轮注入记录。

        同 turn 重复 hook：先摘除自己旧注入再注入新（总量恒 1）；
        过期记录（TTL）一并清扫；别插件对象一律不触碰。记录在
        on_llm_response（宿主请求真实结束，S-01）/after/TTL/停用
        清偿——**不存在跨请求清扫**：其他请求的开始不得清偿本记录
        （仍在途 A 不因 B 失去部件/句柄）。宿主缺
        extra_user_content_parts 的分支不会走到这里（main 入口已
        提前返回，零注入零句柄）。"""
        parts = getattr(request, "extra_user_content_parts", None)
        if parts is None:
            raise ValueError("宿主缺少 extra_user_content_parts 能力；"
                             "调用方应在入口降级，不得注入")
        # 同 turn 旧注入摘除 + TTL 过期记录清扫（都只动自己的对象）
        self._withdraw_turn(turn_identity)
        self._sweep_expired()
        from astrbot.core.agent.message import TextPart

        part = TextPart(text=text).mark_as_temp()
        parts.append(part)
        record = InjectionRecord(
            turn_identity=turn_identity,
            part=part,
            request_ref=weakref.ref(request),
            handle_ids=list(handle_ids), created_ts=_now_ts())
        self._injections.append(record)
        return record

    def _remove_parts(self, record: InjectionRecord) -> None:
        """从记录的 request（若仍存活）中按 `is` 摘除自己的 part。"""
        if record.part is None:
            return
        request = record.request_ref() if record.request_ref else None
        if request is None:
            return
        parts = getattr(request, "extra_user_content_parts", None)
        if parts is None:
            return
        target = record.part
        parts[:] = [p for p in parts if p is not target]

    def _release(self, records: list[InjectionRecord]) -> None:
        """丢弃记录并按记录在案 handle_id 触发句柄清偿回调（同步；
        不触碰他人对象，也不影响并发同 turn 新注册的句柄）。"""
        if not records:
            return
        handle_ids: list[str] = []
        for rec in records:
            self._remove_parts(rec)
            for hid in rec.handle_ids:
                if hid not in handle_ids:
                    handle_ids.append(hid)
        dropped = {id(rec) for rec in records}
        self._injections = [r for r in self._injections
                            if id(r) not in dropped]
        if handle_ids and self._on_release is not None:
            self._on_release(handle_ids)

    def _withdraw_turn(self, turn_identity: str) -> None:
        self._release([r for r in self._injections
                       if r.turn_identity == turn_identity])

    def _sweep_expired(self) -> None:
        now = _now_ts()
        self._release([r for r in self._injections
                       if now - r.created_ts > self._record_ttl])

    def expire_turn(self, turn_identity: str | None = None) -> None:
        """turn 收尾：摘除自己注入的 part 并丢弃记录（仅本插件对象）。

        S-01：由 on_llm_response（宿主真实结束信号）与
        after_message_sent 按本 turn 调用；不做跨请求清扫——他 turn
        记录（无论是否在途）一律不动。"""
        if turn_identity is None:
            self._release(list(self._injections))
            return
        self._withdraw_turn(turn_identity)
