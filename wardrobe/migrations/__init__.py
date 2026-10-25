"""版本化数据库迁移。

- 迁移按版本号递进，禁止跳迁；应用前先做文件级一致性备份。
- schema_version 大于已知版本时拒绝写入（SchemaTooNew），提示使用对应插件版本。
- 每个迁移是独立模块，SQL 全部为受控字面量，不拼接运行时输入。
"""

from __future__ import annotations

MIGRATIONS: dict[int, str] = {}

KNOWN_SCHEMA_VERSION = 0  # 由迁移模块注册时更新


def register(version: int, statements: str) -> None:
    global KNOWN_SCHEMA_VERSION
    if version in MIGRATIONS:
        raise RuntimeError(f"迁移版本 {version} 重复注册")
    MIGRATIONS[version] = statements
    if version > KNOWN_SCHEMA_VERSION:
        KNOWN_SCHEMA_VERSION = version


def pending_versions(current: int) -> list[int]:
    return sorted(v for v in MIGRATIONS if v > current)


# 注册迁移模块（放在文件末尾以避免部分初始化问题）
from . import m0001_initial as _m0001  # noqa: E402,F401
from . import m0002_auth_generations as _m0002  # noqa: E402,F401
from . import m0003_autonomy_usage as _m0003  # noqa: E402,F401
from . import m0004_turn_usage as _m0004  # noqa: E402,F401
