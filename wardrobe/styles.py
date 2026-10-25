"""风格档案、候选排序（M2 任务#012 P-04 / #013 Q-07）。

风格是持久偏好与可解释的确定性排序：avoid 标签明确排除，preferred
与场景按可解释、稳定次序排序，同分以类别稳定序 + item_id 字典序收
尾；分页固定大小、翻页可重现。场景偏好参与排序并在候选解释中可见。
不引入向量、Rerank 或任何远程检索。风格说明与服装描述都是不可信数
据——只作为检索键与投影文本，绝不升级为系统指令或权限。

Q-07：字段清洗在本层完成（仅 provided 字段）；增量合并发生在
repository 写事务内对最新档案（并发的不同字段请求互不抹掉）。
"""

from __future__ import annotations

from .models import (
    ALL_CATEGORIES,
    InvalidInput,
    clean_text,
)
from .config import LIST_PAGE_SIZE, STYLE_NOTE_MAX_LEN

STYLE_TAG_MAX = 12
STYLE_TAG_LIST_MAX = 10
STYLE_SCENE_MAX_LEN = 100

_STYLE_FIELDS = ("喜欢", "避开", "场景", "说明")


def parse_style_args(rest: str) -> dict:
    """`/衣柜 风格` 设置语法：`喜欢 a,b,c 避开 x,y 场景 文字 说明 文字`。

    键值对按出现顺序解析，允许任意子集与顺序；重复键取最后一次；
    未知键拒绝；值保持原始中文与空格（清洗在 service 层统一做）。
    """
    text = (rest or "").strip()
    if not text:
        return {}
    fields: dict[str, str] = {}
    current: str | None = None
    buffer: list[str] = []
    for token in text.split():
        if token in _STYLE_FIELDS:
            if current is not None:
                fields[current] = " ".join(buffer).strip()
            current = token
            buffer = []
        elif current is not None:
            buffer.append(token)
        else:
            raise InvalidInput(
                "风格设置需以 喜欢/避开/场景/说明 开头，"
                "例：风格 喜欢 连衣裙,优雅 避开 运动 场景 日常")
    if current is not None:
        fields[current] = " ".join(buffer).strip()
    unknown = set(fields) - set(_STYLE_FIELDS)
    if unknown:
        raise InvalidInput(f"未知的风格字段：{'、'.join(sorted(unknown))}")
    return fields


def clean_style_tags(raw: str | list[str] | None, field_name: str) -> list[str]:
    """风格标签：逗号分隔或列表；逐项清洗、去重保序、限长限数。"""
    if raw is None or raw == "":
        return []
    if isinstance(raw, str):
        parts = [p for p in (s.strip() for s in raw.split(",")) if p]
    elif isinstance(raw, list):
        parts = []
        for item in raw:
            if not isinstance(item, str):
                raise InvalidInput(f"{field_name}标签必须是字符串列表")
            parts.append(item.strip())
    else:
        raise InvalidInput(f"{field_name}标签必须是逗号分隔文本或列表")
    cleaned: list[str] = []
    for part in parts:
        tag = clean_text(part, field_name=f"{field_name}标签",
                         max_len=STYLE_TAG_MAX, allow_empty=False,
                         allow_symbols=False)
        if tag not in cleaned:
            cleaned.append(tag)
    if len(cleaned) > STYLE_TAG_LIST_MAX:
        raise InvalidInput(
            f"{field_name}标签至多 {STYLE_TAG_LIST_MAX} 个")
    return cleaned


class StyleService:
    """风格档案读取 + 候选排序（同角色跨窗口共享；另角色隔离由 repo 保证）。

    Q-07：写入增量在 repository 最终事务内合并；本层提供 provided 字段
    的清洗 helpers（service 调用后再传键值给 repo）。
    """

    def __init__(self, repository, config) -> None:
        self._repo = repository
        self._config = config

    # -- provided 字段清洗（Q-07：只清洗出现的字段） --------------------

    def clean_tags_value(self, raw, field_name: str) -> list[str]:
        return clean_style_tags(raw, field_name)

    def clean_scene(self, raw) -> str:
        if raw is None:
            return ""
        if not isinstance(raw, str):
            raise InvalidInput("场景偏好必须是文本")
        return clean_text(raw, field_name="场景偏好",
                          max_len=STYLE_SCENE_MAX_LEN, allow_empty=True)

    def clean_note(self, raw) -> str:
        if raw is None:
            return ""
        if not isinstance(raw, str):
            raise InvalidInput("风格说明必须是文本")
        return clean_text(raw, field_name="风格说明",
                          max_len=STYLE_NOTE_MAX_LEN, allow_empty=True)

    # -- 读取 -----------------------------------------------------------

    async def get(self, role_id: str) -> dict:
        row = await self._repo.get_style_profile(role_id)
        if row is None:
            return {"preferred_tags": [], "avoid_tags": [],
                    "scene_preferences": "", "note": "", "revision": 0}
        import json

        try:
            preferred = json.loads(row["preferred_tags_json"] or "[]")
            avoid = json.loads(row["avoid_tags_json"] or "[]")
        except (TypeError, ValueError):
            preferred, avoid = [], []
        return {"preferred_tags": preferred, "avoid_tags": avoid,
                "scene_preferences": row["scene_preferences"] or "",
                "note": row["note"] or "", "revision": row["revision"]}

    # -- 确定性候选排序 --------------------------------------------------

    def rank(self, items: list[dict], profile: dict,
             query: str, page: int) -> dict:
        """确定性候选排序：avoid 排除 → query 过滤 → preferred 命中降序
        → 场景命中降序（Q-07：场景偏好参与排序与解释）→ 类别稳定序 →
        item_id 字典序；同分规则固定，翻页可重现。"""
        if not items:
            return {"status": "empty_wardrobe", "candidates": [], "page": 1,
                    "total_pages": 1, "total": 0}
        preferred = set(profile.get("preferred_tags") or [])
        avoid = set(profile.get("avoid_tags") or [])
        scene = profile.get("scene_preferences") or ""
        scene_tokens = [s for s in scene.replace("，", " ").split() if s]
        q = (query or "").strip().lower()
        category_order = {c: i for i, c in enumerate(ALL_CATEGORIES)}
        scored: list[tuple] = []
        for it in items:
            tags = set(it.get("tags") or [])
            if tags & avoid:
                continue  # avoid 明确排除
            haystack = " ".join(
                [it.get("name") or "", it.get("description") or "",
                 " ".join(sorted(tags))]).lower()
            if q and q not in haystack:
                continue  # query 是检索条件：不命中即排除
            preferred_hits = len(tags & preferred)
            scene_hit = len([t for t in scene_tokens
                             if t.lower() in haystack])
            scored.append((-preferred_hits, -scene_hit,
                           category_order.get(it.get("category"), 99),
                           it.get("item_id") or "", it, preferred_hits,
                           scene_hit))
        if not scored:
            return {"status": "no_match", "candidates": [], "page": 1,
                    "total_pages": 1, "total": 0}
        scored.sort(key=lambda t: t[:5])
        ordered = scored
        total = len(ordered)
        total_pages = max(1, (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
        page = max(1, min(page, total_pages))
        start = (page - 1) * LIST_PAGE_SIZE
        candidates = [
            {"item_id": entry[4]["item_id"], "name": entry[4]["name"],
             "category": entry[4]["category"],
             "has_image": bool(entry[4].get("has_image")),
             "matched_tags": sorted(set(entry[4].get("tags") or [])
                                    & preferred),
             "matched_scene": [token for token in scene_tokens
                               if token.lower() in " ".join(
                                   [entry[4].get("name") or "",
                                    entry[4].get("description") or "",
                                    " ".join(sorted(
                                        entry[4].get("tags") or []))]
                               ).lower()],
             "score_reason": (
                 f"preferred×{entry[5]} scene×{entry[6]}"
                 if (entry[5] or entry[6]) else "base")}
            for entry in ordered[start:start + LIST_PAGE_SIZE]]
        return {"status": "ok", "candidates": candidates, "page": page,
                "total_pages": total_pages, "total": total}
