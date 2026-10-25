"""自主行为开关与用量视图（M2 任务#012 P-03 / #013 Q-03/Q-04）。

许可判定与用量消耗的权威路径在 repository 写事务内（autonomy_gate，
BEGIN 后原子核验+消耗，失败回滚零副作用）；本模块只提供跨请求的
状态视图（`/衣柜 自主` 查看与 cw_get_state 共用）与命令语义解析。
所有时间取服务器 UTC，模型提供的任何时间一律不参与判定。

Q-03：`tool_write_available` 如实反映"宿主未提供可信工具调用标识"
的限制——状态/投影不宣称自主写入可用又在调用时 unknown。
Q-04：只读"今日"展示在 day_utc 非今日时显示 0（不沿用昨日计数）。
"""

from __future__ import annotations

from datetime import datetime, timezone

from .models import InvalidInput

# Q-03：该固定宿主（v4.28.0/a4121464）全部插件可达 hook 均不含
# tool_call_id——AI 写入在正式入口 fail-closed（见 llm_tools）。
TOOL_WRITE_UNAVAILABLE_REASON = "宿主未提供可信工具调用标识（AI 写入暂不可用）"


def parse_switch_args(rest: str) -> dict:
    """`/衣柜 自主` 参数：``（查看）/ `on|off` / `add on|off` /
    `wear on|off`。返回 {master?/add?/wear?} 更新字典或空（查看）。"""
    text = (rest or "").strip().lower()
    if not text:
        return {}
    tokens = text.replace("：", " ").split()
    if len(tokens) == 1 and tokens[0] in ("on", "off"):
        return {"master": tokens[0] == "on"}
    if len(tokens) == 2 and tokens[0] in ("add", "添衣") \
            and tokens[1] in ("on", "off"):
        return {"add": tokens[1] == "on"}
    if len(tokens) == 2 and tokens[0] in ("wear", "选穿") \
            and tokens[1] in ("on", "off"):
        return {"wear": tokens[1] == "on"}
    raise InvalidInput(
        "自主开关用法：自主 on|off；自主 add on|off；自主 wear on|off")


def cooldown_remaining(usage: dict, cooldown_seconds: int,
                       now: datetime | None = None) -> int:
    """自主添衣冷却剩余秒数（0=可立即提交；无记录/冷却关闭=0）。

    Q-04：冷却与 UTC 日界独立——跨日后 last_add_at 仍参与判定。"""
    if cooldown_seconds <= 0:
        return 0
    last = usage.get("last_add_at") if usage else None
    if not last:
        return 0
    try:
        last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
    except ValueError:
        return 0
    now = now or datetime.now(timezone.utc)
    remaining = cooldown_seconds - (now - last_dt).total_seconds()
    if remaining <= 0:
        return 0
    return int(remaining) + 1


async def describe(service, role_id: str, umo: str) -> dict:
    """开关 + 用量 + 窗口 AI 许可只读视图（命令查看与 cw_get_state 共用）。

    任务#016 S-01：附自动投影安全关闭的如实披露（静态宿主契约判定，
    与 R-03 的 tool_write_available 同模式——不宣称可用/开启）。"""
    from .host_adapter import auto_projection_support

    proj_enabled, proj_reason = auto_projection_support()
    row = await service.repo.get_autonomy(role_id)
    state = {
        "master_enabled": bool(row["master_enabled"]) if row is not None else True,
        "add_enabled": bool(row["add_enabled"]) if row is not None else True,
        "wear_enabled": bool(row["wear_enabled"]) if row is not None else True,
        "revision": row["revision"] if row is not None else 0,
    }
    grant = await service.repo.get_grant(role_id, umo)
    usage = await service.repo.read_usage_snapshot(role_id)
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    # Q-04：展示按今日事实——day_utc 非今日时计数显示 0（不沿用昨日）
    is_today = usage.get("day_utc") == today
    return {
        **state,
        "tool_write_available": False,
        "tool_write_reason": TOOL_WRITE_UNAVAILABLE_REASON,
        "auto_projection_enabled": proj_enabled,
        "auto_projection_reason": "" if proj_enabled else proj_reason,
        "allow_ai_mutation": bool(grant["allow_ai_mutation"])
        if grant is not None else False,
        "day_utc": usage.get("day_utc") or today,
        "add_count": (usage.get("add_count") or 0) if is_today else 0,
        "daily_add_limit": service.config.autonomy_daily_add_limit,
        "add_cooldown_remaining_s": cooldown_remaining(
            usage, service.config.autonomy_cooldown_seconds, now),
        "wear_count": (usage.get("wear_count") or 0) if is_today else 0,
    }
