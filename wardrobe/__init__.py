"""AstrBot 角色小衣柜领域包。

不含宿主依赖的领域实现：数据模型、配置、SQLite 仓储、角色映射、
权限、图片安全流水线、穿搭与统一服务。宿主适配边界见 host_adapter，
宿主入口见仓库根 main.py。
"""

from .config import WardrobeConfig
from .service import WardrobeService

__all__ = ["WardrobeConfig", "WardrobeService"]
