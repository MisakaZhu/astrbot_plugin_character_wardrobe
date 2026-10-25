"""迁移 v4：持久 turn 预算（任务#013 Q-04）。

- autonomy_turn_usage：每 (role, turn, action) 一行。turn_key 是完整
  可信 turn 归属（platform|umo|sender_ref|message_id 的 sha256），
  与业务写入同事务 INSERT——同一 turn 的第二次同动作写入因主键冲突
  在事务内被拒（重启后依然成立；两窗口同 message_id 因 umo/actor
  不同互不冲突，日额度按角色共享）。
- 兼容：v1/v2/v3 数据与结构不变；不编辑已发布候选迁移。
"""

from __future__ import annotations

from . import register as mig_register

SQL = """
CREATE TABLE autonomy_turn_usage (
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    turn_key TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('add', 'wear')),
    created_at TEXT NOT NULL,
    PRIMARY KEY (role_id, turn_key, action)
);
"""

mig_register(4, SQL)
