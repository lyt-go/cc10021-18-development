"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import exchange, resource

# 交换快照路由含更具体的固定路径，必须在通配资源路由之前注册，
# 否则 /api/table_catalog/exchange/... 会被 /api/{module} 抢先匹配。
ROUTERS = [exchange, resource]
