"""业务路由汇总：专用投递追溯接口先注册，再注册通用资源路由。"""
from __future__ import annotations

from app.routers import delivery_ops, resource

ROUTERS = [delivery_ops, resource]
