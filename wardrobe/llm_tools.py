"""模型工具核心层（M2 任务#012 P-02 / #013 Q-02/Q-03/Q-08）。

main.py 用官方 `filter.llm_tool` 装饰器登记四个薄 handler，加载器
（star_manager L1253-1298）以 `functools.partial(raw, plugin)` 绑定
实例后按 `handler(event, **params)` 派发；身份/角色/权限/UMO 一律由
服务端从真实 event 派生，模型参数仅限各工具的白名单字段。返回值是
**受控结构化 JSON 字符串**（确定性序列化），只在真实提交已确认时表
达成功；unknown 不猜结果并附脱敏核对凭据。绝不向模型返回路径、URL、
actor 明文、哈希或二进制。

Q-03（fail-closed）：该固定宿主（v4.28.0/a4121464）的全部插件可达
hook 均不含当前调用 tool_call_id（沿派发链核实的结论）——**正式入口
的 AI 写入（cw_add_item/cw_wear_items）实际 rejected**，稳定原因见
autonomy.TOOL_WRITE_UNAVAILABLE_REASON；只读工具、风格/开关手工命令
与 M1 手工添衣/选穿继续。核心层保留显式 `call_id` 参数作为"可信契约
合成分支"（产品入口永不传）：幂等身份=(namespace, role, platform,
umo, actor_ref, turn_identity, call_id, action)+完整规范参数
fingerprint（图片源绑定可信层派生的规范组件身份）。

R-02/S-02：完整规范工具参数指纹**实际传入 service 并用于持久账本
比较/写入与 unknown 核对凭据**（单一来源）——同 call 改
expected_revision/衣服集合/规范业务字段/**已核验组件**（同事件同
字节不同位置也算不同组件，身份=kind:位置:内容摘要）一律
IdempotencyConflict 拒绝，同参完整重放保持原结果且资源/预算只计
一次；同组件重发句柄与跨重启由稳定事实重算同一身份。M1 手工命令
路径不传指纹，维持既有服务端重算语义不变。

Q-08：登记 schema 不替代运行时验证——全部模型输入先经严格类型校验
（str/int 严格区分 bool；非法一律 rejected），零业务/usage/图片副作
用；首返与重放返回同一完整受控业务结果（仅 state 不同）。
"""

from __future__ import annotations

import hashlib
import json

from .autonomy import TOOL_WRITE_UNAVAILABLE_REASON
from .models import (
    ALL_CATEGORIES,
    HandleInvalid,
    IdempotencyConflict,
    ImageRejected,
    InvalidInput,
    ItemNotFound,
    ManualLockActive,
    NotInitialized,
    PermissionDenied,
    RoleUnbound,
    ServiceStopping,
    StaleContext,
    TxOutcomeUnknown,
    VersionConflict,
    canonical_fingerprint,
    clean_tags,
    clean_text,
)
from .config import DESC_MAX_LEN, NAME_MAX_LEN


def _dump(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _rejected(reason: str, **extra) -> str:
    return _dump({"state": "rejected", "reason": reason, **extra})


# ---- Q-08：严格运行时类型验证（任何副作用之前） -----------------------

def _require_str(value, field: str, allow_empty: bool = True) -> str:
    if value is None:
        if not allow_empty:
            raise InvalidInput(f"{field}不能为空")
        return ""
    if not isinstance(value, str):
        raise InvalidInput(f"{field}必须是字符串")
    if not allow_empty and not value.strip():
        raise InvalidInput(f"{field}不能为空")
    return value


def _require_int(value, field: str, default: int | None = None) -> int:
    if value is None:
        if default is None:
            raise InvalidInput(f"{field}不能为空")
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidInput(f"{field}必须是整数")
    if isinstance(value, float):
        if not value.is_integer():
            raise InvalidInput(f"{field}必须是整数")
        return int(value)
    return value


def _require_str_list(value, field: str) -> list[str]:
    if value is None:
        raise InvalidInput(f"{field}不能为空")
    if not isinstance(value, list) or \
            any(not isinstance(i, str) for i in value):
        raise InvalidInput(f"{field}必须是字符串数组")
    return value


def turn_request_key(namespace: str, role_id: str, platform: str, umo: str,
                     sender_ref: str, turn_identity: str, call_id: str,
                     action: str) -> str:
    """Q-03：幂等身份——安装/角色/平台/窗口/actor/可信 turn/**实际
    call ID**/动作全绑定（不含参数）。完整规范参数 fingerprint 随账本
    单独保存，作为**同 call 异参**的冲突判据（M1 账本既有语义：同键
    不同 fingerprint → IdempotencyConflict）。"""
    raw = "|".join([namespace, role_id, platform, umo, sender_ref,
                    turn_identity, call_id, action])
    return "cw_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def turn_fingerprint(action: str, **params) -> str:
    return canonical_fingerprint(f"cw_{action}", **params)


def _canonical_image_identity(handle) -> str:
    """S-02：可信层派生的**规范组件身份**（进入指纹的单一判据）。

    无图 → "no-image"；有图 → ``img:{已核验kind}:{组件位置}:{payload
    内容摘要}``。同一组件=同事件同 kind 同位置同字节：同 turn 重复
    hook 重发句柄、跨重启重新核验，都能由句柄的稳定事实重算出同一
    身份（重放一致）；同事件不同位置（即便字节相同）、不同字节、
    不同 kind 一律**不同组件**（异参冲突）。不使用随机 handle 值，
    不含真实 payload/路径/签名，模型不可提供身份；图片资产的**内容
    去重**（服务层按 processed_sha256）与此判据独立。
    """
    if handle is None:
        return "no-image"
    kind = str(getattr(handle, "kind", "") or "")
    index = getattr(handle, "component_index", -1)
    payload = getattr(handle, "payload", "") or ""
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"img:{kind}:{index}:{digest}"


async def tool_list_items(service, persona_key, scope, actor_ref, umo,
                          is_host_admin, query, category, page) -> str:
    """cw_list_items：有限 query/category/page 检索（reader 级）。"""
    try:
        query = _require_str(query, "检索词")
        category = _require_str(category, "类别")
        page = _require_int(page, "页码", default=1)
        if category and category not in ALL_CATEGORIES:
            return _rejected(f"类别只能是 {'、'.join(ALL_CATEGORIES)}")
        if page < 1 or page > 10000:
            return _rejected("页码必须是 1-10000 的整数")
        result = await service.style_candidates(
            persona_key, scope, actor_ref, umo, is_host_admin,
            query=query, page=page, category=category or None)
        return _dump({"state": "ok", **result})
    except (RoleUnbound, NotInitialized) as exc:
        return _rejected(str(exc))
    except (PermissionDenied, InvalidInput, ItemNotFound, StaleContext,
            ServiceStopping) as exc:
        return _rejected(str(exc))
    except Exception:
        return _rejected("内部错误；请稍后重试或使用手工命令")


async def tool_get_state(service, persona_key, scope, actor_ref, umo,
                         is_host_admin) -> str:
    """cw_get_state：风格/当前穿搭/revision/开关/锁/自主可用原因。"""
    try:
        view = await service.current_outfit(
            persona_key, scope, actor_ref, umo, is_host_admin)
        style = await service.style_get(
            persona_key, scope, actor_ref, umo, is_host_admin)
        autonomy_view = await service.autonomy_get(
            persona_key, scope, actor_ref, umo, is_host_admin)
        # 手工锁剩余分钟（服务器过期事实；权威判定在写事务内）
        lock_until = view.get("manual_lock_until")
        locked = False
        lock_remaining_min = 0
        if lock_until:
            from datetime import datetime, timezone

            from .outfits import lock_active

            if lock_active(lock_until):
                locked = True
                deadline = datetime.fromisoformat(
                    str(lock_until).replace("Z", "+00:00"))
                remaining = (deadline -
                             datetime.now(timezone.utc)).total_seconds() / 60
                lock_remaining_min = max(0, int(remaining) + 1)
        reasons = []
        if not autonomy_view["master_enabled"]:
            reasons.append("角色自主行为总开关已关闭")
        if not autonomy_view["add_enabled"]:
            reasons.append("自主添衣已单独关闭")
        if not autonomy_view["wear_enabled"]:
            reasons.append("自主选穿已单独关闭")
        if not autonomy_view["allow_ai_mutation"]:
            reasons.append("当前窗口未允许角色自主变更")
        if locked:
            reasons.append("手工穿搭锁生效中")
        if not autonomy_view.get("tool_write_available", False):
            reasons.append(autonomy_view.get(
                "tool_write_reason", TOOL_WRITE_UNAVAILABLE_REASON))
        return _dump({
            "state": "ok",
            "outfit": {"revision": view.get("revision"),
                       "items": view.get("items", [])},
            "style": {"preferred_tags": style["preferred_tags"],
                      "avoid_tags": style["avoid_tags"],
                      "revision": style["revision"]},
            "autonomy": {
                "master_enabled": autonomy_view["master_enabled"],
                "add_enabled": autonomy_view["add_enabled"],
                "wear_enabled": autonomy_view["wear_enabled"],
                "add_count_today": autonomy_view["add_count"],
                "daily_add_limit": autonomy_view["daily_add_limit"],
                "add_cooldown_remaining_s":
                    autonomy_view["add_cooldown_remaining_s"],
                "tool_write_available":
                    autonomy_view.get("tool_write_available", False),
                # 任务#016 S-01：自动投影安全关闭的如实披露（静态宿主
                # 契约判定；与 tool_write_available 同源视图）。
                "auto_projection_enabled":
                    autonomy_view.get("auto_projection_enabled", False),
                "auto_projection_reason":
                    autonomy_view.get("auto_projection_reason", ""),
                "usable": not reasons,
                "unavailable_reasons": reasons,
            },
            "manual_lock": {"active": locked,
                            "remaining_minutes": lock_remaining_min},
        })
    except (RoleUnbound, NotInitialized) as exc:
        return _rejected(str(exc))
    except (PermissionDenied, InvalidInput, StaleContext,
            ServiceStopping) as exc:
        return _rejected(str(exc))
    except Exception:
        return _rejected("内部错误；请稍后重试或使用手工命令")


async def tool_add_item(service, persona_key, scope, actor_ref, umo,
                        is_host_admin, platform, turn_identity: str,
                        name, category, description, tags,
                        handle=None, call_id: str = "",
                        host_verify=None) -> str:
    """cw_add_item 核心：文字/可信句柄图片添衣（source_kind="ai"）。

    Q-03：`call_id` 缺失（产品入口恒缺）→ 实际 rejected，零副作用；
    显式 call_id 仅供已证明可信契约的合成分支（测试分列）。
    handle 是已由 main.py 经 HandleRegistry.get_verified 复核的真实
    句柄；未提供 → 无图文字条目。
    """
    try:
        if not call_id:
            return _rejected(TOOL_WRITE_UNAVAILABLE_REASON)
        name_c = clean_text(_require_str(name, "名称", allow_empty=False),
                            field_name="名称", max_len=NAME_MAX_LEN,
                            allow_empty=False, allow_symbols=False)
        category_c = _require_str(category, "类别", allow_empty=False)
        if category_c not in ALL_CATEGORIES:
            return _rejected(f"类别只能是 {'、'.join(ALL_CATEGORIES)}")
        desc_c = clean_text(_require_str(description, "描述"),
                            field_name="描述", max_len=DESC_MAX_LEN,
                            allow_empty=True)
        tags_c = clean_tags(_require_str(tags, "标签") or [])
        handles = [handle] if handle is not None else []
        # R-02/S-02：完整规范工具参数指纹**实际传入 service**——图片源
        # 判据为可信层派生的规范组件身份（kind/位置/内容摘要），账本
        # 比较/写入与 unknown 核对凭据使用同一份（单一来源）。
        fingerprint = turn_fingerprint(
            "add", name=name_c, category=category_c, description=desc_c,
            tags=tags_c, image_source=_canonical_image_identity(handle))
        request_key = turn_request_key(
            service.repo.namespace, scope.role_id, platform, umo, actor_ref,
            turn_identity, call_id, "add")
        result = await service.add_item(
            persona_key, scope, actor_ref, umo, is_host_admin,
            name=name_c, description=desc_c, category=category_c,
            tags=tags_c, layerable=False, source_kind="ai",
            handles=handles, request_key=request_key,
            host_verify=host_verify, fingerprint=fingerprint,
            autonomy_check=service.repo.autonomy_gate(
                scope.role_id, "add", turn_identity, umo))
        # Q-08：首返/重放同源同形——完整账本结果 + state 标志
        state = "replayed" if result.get("idempotent_replay") else "committed"
        payload = {"state": state}
        payload.update({k: v for k, v in result.items()
                        if k != "idempotent_replay"})
        return _dump(payload)
    except TxOutcomeUnknown as exc:
        credential = getattr(exc, "credential", None)
        payload = {"state": "unknown",
                   "reason": "提交结果无法确认；本轮不再重复提交，"
                             "可稍后由用户以 /衣柜 核对 凭据确认"}
        if credential:
            payload["credential"] = credential
        return _dump(payload)
    except (ImageRejected, PermissionDenied, InvalidInput, ItemNotFound,
            StaleContext, ServiceStopping, HandleInvalid,
            IdempotencyConflict) as exc:
        return _rejected(str(exc))
    except Exception:
        return _rejected("内部错误；请稍后重试或使用手工命令")


async def tool_wear_items(service, persona_key, scope, actor_ref, umo,
                          is_host_admin, platform, turn_identity: str,
                          item_ids, expected_revision, call_id: str = "",
                          host_verify=None) -> str:
    """cw_wear_items 核心：本角色有效 ID + expected_revision 原子选穿。

    Q-03：call_id 缺失 → 实际 rejected（同 add）。"""
    try:
        if not call_id:
            return _rejected(TOOL_WRITE_UNAVAILABLE_REASON)
        cleaned = _require_str_list(item_ids, "服装ID列表")
        expected = _require_int(expected_revision, "穿搭版本")
        # R-02：完整规范工具参数（含 expected_revision）**实际传入
        # service**——同 call 异参（如改版本/改集合）经账本指纹冲突
        # 拒绝，同参完整重放保持；unknown 核对凭据与账本同源。
        fingerprint = turn_fingerprint("wear", item_ids=sorted(cleaned),
                                       expected_revision=expected)
        request_key = turn_request_key(
            service.repo.namespace, scope.role_id, platform, umo, actor_ref,
            turn_identity, call_id, "wear")
        result = await service.wear(
            persona_key, scope, actor_ref, umo, is_host_admin,
            item_ids=cleaned, expected_revision=expected,
            source_kind="ai", request_key=request_key,
            host_verify=host_verify, fingerprint=fingerprint,
            autonomy_check=service.repo.autonomy_gate(
                scope.role_id, "wear", turn_identity, umo))
        state = "replayed" if result.get("idempotent_replay") else "committed"
        payload = {"state": state}
        payload.update({k: v for k, v in result.items()
                        if k != "idempotent_replay"})
        return _dump(payload)
    except TxOutcomeUnknown as exc:
        credential = getattr(exc, "credential", None)
        payload = {"state": "unknown",
                   "reason": "提交结果无法确认；本轮不再重复提交，"
                             "可稍后由用户以 /衣柜 核对 凭据确认"}
        if credential:
            payload["credential"] = credential
        return _dump(payload)
    except (ManualLockActive, VersionConflict, ImageRejected,
            PermissionDenied, InvalidInput, ItemNotFound, StaleContext,
            ServiceStopping, IdempotencyConflict) as exc:
        return _rejected(str(exc))
    except Exception:
        return _rejected("内部错误；请稍后重试或使用手工命令")
