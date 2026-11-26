"""分布式采集调度与反封锁平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store

app = FastAPI(title="分布式采集调度与反封锁平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)


def seed_delivery_receipt_demo() -> None:
    """预置题面中的四项部分签收交接，便于页面直接核对完整流程。"""
    from app.services import SERVICES

    delivery = SERVICES["delivery_pipe"]
    if delivery.list_handoffs():
        return
    delivery.create_four_item_scenario(
        entry_id=3,
        export_id=3,
        batch_id="BATCH-PARTIAL-4",
        operator="交接员",
        remark="两项已签收、一项明确失败、一项确认未知",
    )


seed_delivery_receipt_demo()
