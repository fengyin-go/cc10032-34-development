"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

reconcile 路由含 /api/agg/* 固定前缀，必须先于通用资源路由注册，
否则 /api/agg/batches 会被 /api/{module}/{entry_id} 抢占。
"""
from __future__ import annotations

from app.routers import reconcile, resource

ROUTERS = [reconcile, resource]
