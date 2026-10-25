"""类型、状态、输入校验与错误类型。

所有领域数据结构在此定义；错误层级供命令层转换为用户可读提示。
本包不依赖 AstrBot 宿主，纯 Python 可独立测试。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

# ---------------------------------------------------------------------------
# 受控类别（有限枚举，不能由模型或用户自由扩展）
# ---------------------------------------------------------------------------

class Category(str, Enum):
    TOP = "上装"
    BOTTOM = "下装"
    DRESS = "连衣裙"
    OUTER = "外套"
    SHOES = "鞋"
    ACCESSORY = "配饰"
    SET = "套装"


ALL_CATEGORIES: tuple[str, ...] = tuple(c.value for c in Category)

# 穿着互斥：同一组合里的类别不能同时出现（layerable 服装按 outfits 规则豁免）
MUTEX_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({Category.TOP.value, Category.DRESS.value}),
    frozenset({Category.BOTTOM.value, Category.DRESS.value}),
    frozenset({Category.SET.value, Category.TOP.value}),
    frozenset({Category.SET.value, Category.BOTTOM.value}),
    frozenset({Category.SET.value, Category.DRESS.value}),
)

# 单件上限：同类别至多一件（配饰不限，叠穿由 layerable 豁免）
SINGLE_SLOT_CATEGORIES: frozenset[str] = frozenset(
    {Category.TOP.value, Category.BOTTOM.value, Category.DRESS.value,
     Category.OUTER.value, Category.SHOES.value, Category.SET.value}
)


class SourceKind(str, Enum):
    USER = "user"
    AI = "ai"


class PermissionLevel(str, Enum):
    READER = "reader"
    CONTRIBUTOR = "contributor"
    MAINTAINER = "maintainer"


class CaptionState(str, Enum):
    NONE = "none"        # M1 未接入视觉模型，明确“未自动标注”
    PENDING = "pending"


# ---------------------------------------------------------------------------
# 输入校验（schema 级；资源上限见 config.WardrobeConfig）
# ---------------------------------------------------------------------------

_NAME_MAX = 80
_DESC_MAX = 500
_TAG_MAX_COUNT = 16
_TAG_MAX_LEN = 24

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
# 标签/名称不允许携带路径分隔、协议前缀或代码执行语义
_FORBIDDEN_PATTERN = re.compile(r"(\.\./|\.\.\\|[/\\]|`|\$\{|\||;|&|>|<|data:|file:|http:|https:)")


def utc_now_iso() -> str:
    """持久化一律 UTC ISO-8601；展示层负责换算宿主时区。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_iso_plus_minutes(minutes: int) -> str:
    dt = datetime.now(timezone.utc)
    from datetime import timedelta

    return (dt + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_utc_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class WardrobeError(Exception):
    """领域错误基类。code 用于命令层映射提示，不携带其他角色隐私。"""

    code = "wardrobe_error"

    def __init__(self, message: str = "", **detail: object) -> None:
        super().__init__(message or self.code)
        self.detail = detail


class PermissionDenied(WardrobeError):
    code = "permission_denied"


class RoleUnbound(WardrobeError):
    code = "role_unbound"


class BindingExists(WardrobeError):
    code = "binding_exists"


class NotInitialized(WardrobeError):
    code = "not_initialized"


class RoleDisabled(WardrobeError):
    code = "role_disabled"


class StaleContext(WardrobeError):
    """persona / 授权在请求执行期间变化，旧请求必须作废。"""

    code = "stale_context"


class ItemNotFound(WardrobeError):
    code = "item_not_found"


class InvalidInput(WardrobeError):
    code = "invalid_input"


class QuotaExceeded(WardrobeError):
    code = "quota_exceeded"


class VersionConflict(WardrobeError):
    code = "version_conflict"

    def __init__(self, message: str = "", latest_revision: int = 0) -> None:
        super().__init__(message)
        self.latest_revision = latest_revision


class ManualLockActive(WardrobeError):
    code = "manual_lock_active"

    def __init__(self, message: str = "", until: str | None = None) -> None:
        super().__init__(message)
        self.until = until


class ImageRejected(WardrobeError):
    code = "image_rejected"


class HandleInvalid(WardrobeError):
    code = "handle_invalid"


class IdempotencyConflict(WardrobeError):
    code = "idempotency_conflict"


class SchemaTooNew(WardrobeError):
    code = "schema_too_new"


class ServiceStopping(WardrobeError):
    """服务正在停用：拒绝新请求（H6 停止准入）。"""

    code = "service_stopping"


class TxOutcomeUnknown(WardrobeError):
    """COMMIT 后结果无法确认（H4）：可能已提交也可能未提交。

    调用方不得按“未提交”删除已被 DB 引用的文件；效果可经幂等重放
    （ledger 已提交则返回原结果）或受控恢复（无 DB 行文件清理）结算。
    """

    code = "tx_outcome_unknown"


def clean_text(value: object, *, field_name: str, max_len: int,
               allow_empty: bool = False, allow_symbols: bool = False) -> str:
    """文本清洗：类型、控制字符、长度统一校验。

    allow_symbols=False（默认）时拒绝路径分隔、协议前缀与代码执行语义。
    """
    if not isinstance(value, str):
        raise InvalidInput(f"{field_name} 必须是文本", field=field_name)
    text = _CONTROL_CHARS.sub("", value).strip()
    if not text and not allow_empty:
        raise InvalidInput(f"{field_name} 不能为空", field=field_name)
    if len(text) > max_len:
        raise InvalidInput(f"{field_name} 超过 {max_len} 字上限", field=field_name)
    if not allow_symbols and text and _FORBIDDEN_PATTERN.search(text):
        raise InvalidInput(f"{field_name} 含不允许的路径/协议/符号", field=field_name)
    return text


def clean_tags(value: object) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        parts = [p for p in re.split(r"[,，\s]+", value) if p]
    elif isinstance(value, (list, tuple)):
        parts = []
        for raw in value:
            if not isinstance(raw, str):
                raise InvalidInput("标签必须是文本", field="tags")
            parts.extend(p for p in re.split(r"[,，\s]+", raw) if p)
    else:
        raise InvalidInput("标签必须是文本或列表", field="tags")
    cleaned: list[str] = []
    import unicodedata

    for tag in parts:
        tag = unicodedata.normalize("NFC", _CONTROL_CHARS.sub("", tag)).strip()
        if not tag:
            continue
        if len(tag) > _TAG_MAX_LEN:
            raise InvalidInput(f"单个标签超过 { _TAG_MAX_LEN} 字上限", field="tags")
        if _FORBIDDEN_PATTERN.search(tag):
            raise InvalidInput("标签含不允许的路径/协议/符号", field="tags")
        if tag not in cleaned:
            cleaned.append(tag)
    if len(cleaned) > _TAG_MAX_COUNT:
        raise InvalidInput(f"标签最多 {_TAG_MAX_COUNT} 个", field="tags")
    return cleaned


# ---------------------------------------------------------------------------
# 请求上下文（由宿主适配器从真实事件构造；模型不可提供）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ResolvedIdentity:
    """一次事件解析出的可信身份。persona_key 为宿主最终生效人格键。"""

    umo: str
    platform_name: str
    sender_ref: str          # 脱敏 actor 引用（不可逆哈希）
    persona_key: str | None  # None = 未知/未设置人格，必须拒绝
    persona_display: str | None
    is_host_admin: bool
    message_id: str


@dataclass(frozen=True)
class TurnScope:
    """本轮作用域：服务层与最终提交事务都用它复核绑定/授权是否仍然有效。

    auth_generation 取自持久 auth_events 单调序列；任何撤销/重授/清空/重绑
    都会推进该序列，使旧 scope 永远不能复活（R3）。
    """

    role_id: str
    binding_revision: int
    grant_revision: int
    turn_token: str
    umo: str = ""
    sender_ref: str = ""
    auth_generation: int = 0
    is_host_admin: bool = False
    minimum_level: str = "reader"


def canonical_fingerprint(action: str, **params: object) -> str:
    """规范化业务指纹（R7）：键排序的稳定 JSON → SHA-256。

    只纳入业务参数；turn token、时间戳等瞬时差异不进入指纹。
    list/dict 先做递归规范化（键排序、去 None）。
    """
    def normalize(value: object) -> object:
        if isinstance(value, dict):
            return {str(k): normalize(v) for k, v in sorted(value.items(),
                                                            key=lambda kv: str(kv[0]))
                    if v is not None}
        if isinstance(value, (list, tuple)):
            return [normalize(v) for v in value]
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            return str(value)
        return value

    import hashlib
    import json

    payload = json.dumps({"action": action, **{k: normalize(v) for k, v in params.items()}},
                         ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def settlement_credential(request_key: str, fingerprint: str | None) -> str:
    """公开核对凭据（I6）：绑定原请求键与业务指纹的脱敏短码。

    只用于 /衣柜 核对 的只读查询定位：凭据本身不含原始消息 ID、窗口或
    发起者身份（都是不可逆摘要输入）；由原请求键+指纹派生，因此重复
    生成幂等，且无法用它再次执行 add/wear。
    """
    import hashlib

    raw = f"wardrobe-verify|{request_key}|{fingerprint or ''}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def masked_actor_ref(namespace: str, sender_id: str, platform: str) -> str:
    """脱敏 actor 引用：稳定、不可逆，仅用于所有权与审计比较。"""
    import hashlib

    raw = f"{namespace}|{platform}|{sender_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


@dataclass
class SourceHandle:
    """本轮可信图片来源句柄。由宿主适配器从真实消息组件注册。"""

    handle_id: str
    turn_token: str
    umo: str
    sender_ref: str
    role_id: str
    kind: str                 # base64 | local | remote
    payload: str              # base64 数据 / 本地路径 / 远程 URL
    component_index: int
    created_at: str = field(default_factory=utc_now_iso)
    created_ts: float = 0.0  # Q-06：TTL 清扫用的单调时钟戳（注册时注入）


@dataclass
class WardrobeItem:
    item_id: str
    role_id: str
    name: str
    description: str
    category: str
    tags: list[str]
    layerable: bool
    source_kind: str
    created_by_ref: str
    active: bool
    revision: int
    image_asset_ids: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


@dataclass
class OutfitView:
    role_id: str
    revision: int
    item_ids: list[str]
    slots: list[str]
    source_kind: str
    manual_lock_until: str | None
    updated_at: str
