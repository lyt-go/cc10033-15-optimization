"""补投领域服务的语义化模块路径。

实际状态与投递管道共享同一内存台账，避免出现两份批次/回执数据。
"""
from __future__ import annotations

from app.services.delivery_pipe import (
    DeliveryPipeService,
    PartialReceiptService,
    RedeliveryService,
)

redelivery_service = RedeliveryService()
partial_receipt_service = PartialReceiptService()

__all__ = [
    "DeliveryPipeService",
    "RedeliveryService",
    "PartialReceiptService",
    "redelivery_service",
    "partial_receipt_service",
]
