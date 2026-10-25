"""迁移 v3：自主行为持久用量与 turn 预算记账（任务#012 P-03）。

- autonomy_usage：每角色一行。UTC 日计数（add_count/wear_count）、
  每真实 turn（宿主 message_id）各至多一次实际提交的持久记录、
  冷却时间戳。全部消耗在写事务内原子完成，重启后仍是事实源。
- 兼容：v1/v2 数据不变；既有角色首次用量检查时按需惰性建行
  （day_utc 为当前 UTC 日、计数 0），无需回填。
- 日额度/冷却/turn 预算的判定与消耗由 repository 的事务内 gate 执行；
  本迁移只提供持久结构。
"""

from __future__ import annotations

from . import register as mig_register

SQL = """
CREATE TABLE autonomy_usage (
    role_id TEXT PRIMARY KEY REFERENCES roles(role_id) ON DELETE CASCADE,
    day_utc TEXT NOT NULL,
    add_count INTEGER NOT NULL DEFAULT 0 CHECK (add_count >= 0),
    wear_count INTEGER NOT NULL DEFAULT 0 CHECK (wear_count >= 0),
    last_add_message_id TEXT NOT NULL DEFAULT '',
    last_wear_message_id TEXT NOT NULL DEFAULT '',
    last_add_at TEXT,
    last_wear_at TEXT,
    updated_at TEXT NOT NULL
);
"""

mig_register(3, SQL)
