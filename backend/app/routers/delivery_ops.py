"""投递回执、补投范围与导出追溯的专用 HTTP 接口。

通用资源动作仍保留；这里提供需要传递条目集合/回执内容的细粒度接口。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.services import service_for

router = APIRouter(prefix="/api", tags=["投递回执与补投追溯"])


def delivery_service():
    return service_for("delivery_pipe")


def export_service():
    return service_for("export_dataset")


async def json_body(request: Request) -> dict[str, Any]:
    try:
        data = await request.json()
    except Exception:
        return {}
    if data is None:
        return {}
    if isinstance(data, dict):
        return data
    return {"items": data}


def not_found(message: str = "资源不存在") -> dict[str, Any]:
    return {"ok": False, "error": "not_found", "message": message}


# ── 交接批次 ────────────────────────────────────────

@router.post("/delivery/handoffs")
async def create_delivery_handoff(request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    return delivery_service().create_handoff(payload)


@router.post("/delivery/scenarios/four-items")
async def create_four_item_scenario(request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    return delivery_service().create_four_item_scenario(payload)


@router.get("/delivery/handoffs")
async def list_delivery_handoffs(pipe_id: int | None = None) -> dict[str, Any]:
    items = delivery_service().list_handoffs(pipe_id)
    return {"items": items, "total": len(items)}


@router.get("/delivery/handoffs/{handoff_id}")
async def get_delivery_handoff(handoff_id: str) -> dict[str, Any]:
    handoff = delivery_service().get_handoff(handoff_id=handoff_id)
    return handoff or not_found("部分签收详情不存在")


@router.get("/delivery/trace")
async def query_delivery_trace(batch_id: str | None = None,
                               export_id: int | None = None) -> dict[str, Any]:
    return delivery_service().trace_batch(batch_id=batch_id, export_id=export_id)


@router.get("/delivery/batches/{batch_id}/trace")
async def get_batch_trace(batch_id: str, export_id: int | None = None) -> dict[str, Any]:
    return delivery_service().trace_batch(batch_id=batch_id, export_id=export_id)


@router.get("/delivery/batches/{batch_id}/eligibility")
async def get_batch_eligibility(batch_id: str) -> dict[str, Any]:
    return delivery_service().preview_redelivery(batch_id=batch_id)


# ── 回执 ────────────────────────────────────────────

@router.post("/delivery/receipts")
@router.post("/delivery/receipt")
@router.post("/delivery/ack")
@router.post("/receipts")
async def record_delivery_receipt(request: Request) -> dict[str, Any]:
    return delivery_service().record_receipt(await json_body(request))


@router.post("/delivery/unknown-submissions")
async def mark_unknown_submission(request: Request) -> dict[str, Any]:
    return delivery_service().mark_submission_unknown(await json_body(request))


# ── 补投预览/申请/领取 ──────────────────────────────

@router.post("/delivery/redelivery/preview")
@router.post("/redelivery/preview")
@router.post("/redelivery/eligibility")
async def preview_redelivery(request: Request) -> dict[str, Any]:
    return delivery_service().preview_redelivery(await json_body(request))


@router.post("/delivery/redelivery/apply")
@router.post("/redelivery/apply")
@router.post("/redelivery/requests")
async def apply_redelivery(request: Request) -> dict[str, Any]:
    return delivery_service().apply_redelivery(await json_body(request))


@router.post("/delivery/redelivery/claims")
@router.post("/redelivery/claims")
@router.post("/redelivery/claim")
async def claim_redelivery(request: Request) -> dict[str, Any]:
    return delivery_service().claim_redelivery(await json_body(request))


@router.get("/delivery/redelivery/{request_id}")
async def get_redelivery_request(request_id: str) -> dict[str, Any]:
    req = delivery_service()._find_redelivery_request(request_id)  # noqa: SLF001
    return req or not_found("补投申请不存在")


# ── 按投递配置 ID 的兼容路径 ────────────────────────

@router.get("/delivery_pipe/handoffs")
async def list_pipe_handoffs(entry_id: int | None = None) -> dict[str, Any]:
    items = delivery_service().list_handoffs(entry_id)
    return {"items": items, "total": len(items)}


@router.post("/delivery_pipe/handoffs")
async def create_pipe_handoff(request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    return delivery_service().create_handoff(payload)


@router.post("/delivery_pipe/{entry_id}/handoffs")
@router.post("/delivery_pipe/{entry_id}/handoff")
async def create_named_pipe_handoff(entry_id: int, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    payload.setdefault("entry_id", entry_id)
    return delivery_service().create_handoff(payload)


@router.get("/delivery_pipe/{entry_id}/partial-receipt")
async def pipe_partial_receipt(entry_id: int, batch_id: str | None = None,
                               handoff_id: str | None = None) -> dict[str, Any]:
    handoff = delivery_service().get_handoff(
        entry_id=entry_id, batch_id=batch_id, handoff_id=handoff_id)
    return handoff or not_found("该投递管道暂无部分签收交接")


@router.post("/delivery_pipe/{entry_id}/receipts")
@router.post("/delivery_pipe/{entry_id}/receipt")
async def pipe_record_receipt(entry_id: int, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    payload.setdefault("entry_id", entry_id)
    return delivery_service().record_receipt(payload)


@router.post("/delivery_pipe/{entry_id}/redelivery/preview")
async def pipe_preview_redelivery(entry_id: int, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    payload.setdefault("entry_id", entry_id)
    return delivery_service().preview_redelivery(payload)


@router.post("/delivery_pipe/{entry_id}/redelivery/apply")
async def pipe_apply_redelivery(entry_id: int, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    payload.setdefault("entry_id", entry_id)
    return delivery_service().apply_redelivery(payload)


@router.post("/delivery_pipe/{entry_id}/claims")
async def pipe_claim_redelivery(entry_id: int, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    payload.setdefault("entry_id", entry_id)
    return delivery_service().claim_redelivery(payload)


@router.get("/delivery_pipe/{entry_id}/delivery-trace")
async def pipe_delivery_trace(entry_id: int, batch_id: str | None = None) -> dict[str, Any]:
    return delivery_service().trace_batch(entry_id=entry_id, batch_id=batch_id)


# ── 导出端追溯 ──────────────────────────────────────

@router.get("/export_dataset/delivery-trace")
@router.get("/export_dataset/trace")
async def export_delivery_trace(batch_id: str | None = None,
                                export_id: int | None = None) -> dict[str, Any]:
    result = export_service().delivery_trace(export_id, batch_id)
    return result or not_found("导出任务不存在")


@router.get("/export_dataset/delivery-manifest.csv")
async def export_delivery_manifest_csv(batch_id: str | None = None,
                                       export_id: int | None = None) -> StreamingResponse:
    rows = export_service().delivery_manifest_rows(export_id, batch_id)
    output = io.StringIO()
    output.write("﻿")
    fieldnames = ["batch_id", "item_id", "签收结果", "status", "signed", "failed",
                  "waiting_verification", "receipt_ids", "redelivery_ids", "claim_ids", "可补投"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    filename = f"delivery-manifest-{batch_id or export_id or 'all'}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/export_dataset/{entry_id}/delivery-trace")
async def export_entry_delivery_trace(entry_id: int,
                                      batch_id: str | None = None) -> dict[str, Any]:
    result = export_service().delivery_trace(entry_id, batch_id)
    return result or not_found("导出任务不存在")


@router.post("/export_dataset/{entry_id}/attach-delivery")
async def export_attach_delivery(entry_id: int, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    result = export_service().attach_delivery_batch(
        entry_id, payload.get("batch_id"), payload.get("handoff_id"))
    return result or not_found("导出任务不存在")
