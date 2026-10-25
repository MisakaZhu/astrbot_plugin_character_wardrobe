"""配置验证、保守默认与资源硬上限。

全部数值都设有硬上限；`0 的含义` 在字段注释与 _conf_schema.json 中逐一说明，
代码与文档保持同一口径（TECHNICAL.md#tech-limits）。
"""

from __future__ import annotations

from dataclasses import dataclass


class ConfigError(ValueError):
    pass


def _clamp_int(raw: dict, key: str, default: int, lo: int, hi: int) -> int:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"配置项 {key} 必须是整数")
    if value < lo or value > hi:
        raise ConfigError(f"配置项 {key} 超出允许范围 [{lo}, {hi}]")
    return value


def _clamp_bool(raw: dict, key: str, default: bool) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"配置项 {key} 必须是布尔值")
    return value


@dataclass(frozen=True)
class WardrobeConfig:
    """运行配置。硬上限取自 TECHNICAL.md#tech-limits，配置不能越过。"""

    # 每角色衣服件数上限（硬上限 200，不允许“无限”）
    max_items_per_role: int = 200
    # 单图原始读取上限（MiB，硬上限 10；流式超限立即中止）
    max_image_bytes_mb: int = 10
    # 解码像素上限（MP，硬上限 20）
    max_megapixels: int = 20
    # 最长边上限（px，硬上限 8192）
    max_long_edge: int = 8192
    # 图像处理并发（1..8，共享 BoundGate 工人数）
    image_concurrency: int = 2
    # 暂存/发布中图片的 pending 预算（MiB，4..512；进程内计数边界）
    pending_image_budget_mb: int = 64
    # 每请求图片数量上限（H7；1..16，硬上限 16——单请求不可能无限带图）
    max_images_per_request: int = 8
    # 每请求 processed 字节总额（MiB，1..128；与全服务 pending 预算、
    # 每角色已提交配额分开核算）
    per_request_processed_budget_mb: int = 32
    # 图像/下载共享等待队列上限（0..64；0=不排队立即 busy）
    image_queue_capacity: int = 8
    # 并发下载工人（1..8，共享 BoundGate 容量；不再每次新建 Semaphore）
    download_concurrency: int = 2
    # 每角色图片总额（MiB，硬上限 2048；0 不允许——必须有有效上限）
    role_image_budget_mb: int = 200
    # 手工选穿锁（分钟，0 = 手工选穿不落锁；AI 选穿仍受锁约束）
    manual_lock_minutes: int = 30
    # 远程图片下载超时（秒）
    download_timeout_s: int = 15
    # 视觉标注超时（秒，M2 生效；M1 固定 no-op）
    vision_timeout_s: int = 30
    # 是否接受引用（回复）消息里的图片。默认关闭；只有确认适配器
    # 可信转发后由管理员开启。
    allow_reply_image: bool = False
    # 自主行为默认（M2 生效，仅作为新角色 autonomy_state 的初始值落库）
    autonomy_add_enabled: bool = True
    autonomy_wear_enabled: bool = True
    # 每角色 UTC 日自主添衣上限（件；M2，1..50）
    autonomy_daily_add_limit: int = 5
    # 自主添衣冷却（秒；M2，0..3600，0=无冷却）
    autonomy_cooldown_seconds: int = 60
    # 临时投影最多条目数（M2，1..20；总字符另有硬上限截断）
    autonomy_projection_max_items: int = 8
    # 临时投影总字符上限（M2，200..8000；确定性截断并标注）
    autonomy_projection_max_chars: int = 2000
    # 一轮公开给模型的可信句柄数量上限（M2，0..16；0=不投影句柄）
    autonomy_handle_max: int = 4

    @property
    def max_image_bytes(self) -> int:
        return self.max_image_bytes_mb * 1024 * 1024

    @property
    def role_image_budget_bytes(self) -> int:
        return self.role_image_budget_mb * 1024 * 1024

    @property
    def pending_image_budget_bytes(self) -> int:
        return self.pending_image_budget_mb * 1024 * 1024

    @property
    def per_request_processed_budget_bytes(self) -> int:
        return self.per_request_processed_budget_mb * 1024 * 1024

    @staticmethod
    def from_dict(raw: dict | None) -> "WardrobeConfig":
        raw = raw or {}
        return WardrobeConfig(
            max_items_per_role=_clamp_int(raw, "max_items_per_role", 200, 1, 200),
            max_image_bytes_mb=_clamp_int(raw, "max_image_bytes_mb", 10, 1, 10),
            max_megapixels=_clamp_int(raw, "max_megapixels", 20, 1, 20),
            max_long_edge=_clamp_int(raw, "max_long_edge", 8192, 1024, 8192),
            image_concurrency=_clamp_int(raw, "image_concurrency", 2, 1, 8),
            pending_image_budget_mb=_clamp_int(raw, "pending_image_budget_mb", 64, 4, 512),
            max_images_per_request=_clamp_int(raw, "max_images_per_request", 8, 1, 16),
            per_request_processed_budget_mb=_clamp_int(
                raw, "per_request_processed_budget_mb", 32, 1, 128),
            image_queue_capacity=_clamp_int(raw, "image_queue_capacity", 8, 0, 64),
            download_concurrency=_clamp_int(raw, "download_concurrency", 2, 1, 8),
            role_image_budget_mb=_clamp_int(raw, "role_image_budget_mb", 200, 10, 2048),
            manual_lock_minutes=_clamp_int(raw, "manual_lock_minutes", 30, 0, 1440),
            download_timeout_s=_clamp_int(raw, "download_timeout_s", 15, 5, 60),
            vision_timeout_s=_clamp_int(raw, "vision_timeout_s", 30, 5, 60),
            allow_reply_image=_clamp_bool(raw, "allow_reply_image", False),
            autonomy_add_enabled=_clamp_bool(raw, "autonomy_add_enabled", True),
            autonomy_wear_enabled=_clamp_bool(raw, "autonomy_wear_enabled", True),
            autonomy_daily_add_limit=_clamp_int(
                raw, "autonomy_daily_add_limit", AI_ADD_DAILY_LIMIT, 1, 50),
            autonomy_cooldown_seconds=_clamp_int(
                raw, "autonomy_cooldown_seconds", AI_ADD_COOLDOWN_SECONDS, 0, 3600),
            autonomy_projection_max_items=_clamp_int(
                raw, "autonomy_projection_max_items", 8, 1, PROJECTION_MAX_ITEMS),
            autonomy_projection_max_chars=_clamp_int(
                raw, "autonomy_projection_max_chars", 2000, 200, 8000),
            autonomy_handle_max=_clamp_int(raw, "autonomy_handle_max", 4, 0, 16),
        )


# 文本长度硬上限（不可配置，见 models.clean_text）
NAME_MAX_LEN = 80
DESC_MAX_LEN = 500
STYLE_NOTE_MAX_LEN = 300
LIST_PAGE_SIZE = 20
PROJECTION_MAX_ITEMS = 20
OUTFIT_MAX_ITEMS = 8
AI_ADD_DAILY_LIMIT = 5
AI_ADD_COOLDOWN_SECONDS = 60
TURN_AI_ADD_BUDGET = 1
TURN_AI_WEAR_BUDGET = 1
