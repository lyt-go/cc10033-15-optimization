"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    action: str
    remark: str | None = None
    batch_id: str | int | None = None
    handoff_id: str | int | None = None
    request_id: str | int | None = None
    item_id: str | int | None = None
    item_ids: list[Any] | None = None
    selected_item_ids: list[Any] | None = None
    receipt_id: str | None = None
    status: str | None = None
    result: str | None = None
    idempotency_key: str | None = None
    export_id: int | str | None = None
