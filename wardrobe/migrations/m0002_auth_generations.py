"""迁移 v2：授权世代与逐人授权（R2/R3 整改）。

- auth_events：全局单调 AUTOINCREMENT 序列。任何 revoke/regrant/clear/rebind
  都插入事件行；MAX(id) 即当前授权世代。删除授权行再重建不会复用世代，
  因为序列只增不减。
- sender_grants：同一窗口内逐 sender 的贡献/维护级别。v1 的 session_grants
  保留为窗口启用与 AI 变更开关；读取权限仍以存在 session_grants 行为准。
- 兼容：v1 数据不变；既有窗口的维护者由初始化时的 sender_grants 补录迁移
  无法推断（v1 未存 sender 身份），按“仅宿主管理员为维护者”的保守默认，
  管理员可重新逐人授权。
"""

from __future__ import annotations

from . import register as mig_register

SQL = """
CREATE TABLE auth_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_id TEXT,
    kind TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE sender_grants (
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    umo_ref TEXT NOT NULL,
    sender_ref TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('contributor', 'maintainer')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (role_id, umo_ref, sender_ref)
);
CREATE INDEX idx_sender_grants_lookup
    ON sender_grants(role_id, umo_ref, sender_ref);

ALTER TABLE operation_ledger ADD COLUMN fingerprint TEXT;
"""

mig_register(2, SQL.strip())
