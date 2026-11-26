"""部分签收、迟到回执、补投范围与导出追溯的验收测试。"""
from __future__ import annotations

from app.services import SERVICES


delivery = SERVICES["delivery_pipe"]
exports = SERVICES["export_dataset"]


def _setup(batch_id: str):
    pipe, _ = delivery.create_entry({
        "配置编号": f"PIPE-{batch_id}", "目标系统": "HTTP_API", "投递方式": "push",
    })
    export, _ = exports.create_entry({
        "任务编号": f"EXPORT-{batch_id}", "数据来源": batch_id, "导出格式": "CSV",
    })
    handoff = delivery.create_four_item_scenario(
        entry_id=pipe["id"], export_id=export["id"], batch_id=batch_id)
    return pipe, export, handoff


def test_late_receipt_removes_only_late_item_from_redelivery_scope():
    batch = "TEST-LATE-SCOPE"
    _, export, handoff = _setup(batch)
    assert handoff["signed_item_ids"] == [1, 2]
    assert handoff["counts"]["failed"] == 1
    assert handoff["counts"]["unknown"] == 1
    assert handoff["eligible_item_ids"] == [3, 4]

    preview = delivery.preview_redelivery(batch_id=batch, item_ids=[3, 4])
    assert preview["actual_receive_scope"] == [3, 4]

    receipt = delivery.record_receipt(
        batch_id=batch, item_id=4, status="signed", receipt_id="LATE-4-TEST")
    assert receipt["signed_count"] == 3

    preview = delivery.preview_redelivery(batch_id=batch, item_ids=[3, 4])
    assert preview["actual_receive_scope"] == [3]
    assert preview["kept_item_ids"] == [3]
    assert [item["item_id"] for item in preview["removed_items"]] == [4]
    assert preview["removed_items"][0]["reason"] == "late_signed"

    application = delivery.apply_redelivery(
        batch_id=batch, item_ids=[3, 4], idempotency_key=f"apply-{batch}")
    assert application["accepted_item_ids"] == [3]
    assert application["removed_item_ids"] == [4]

    claim = delivery.claim_redelivery(request_id=application["id"])
    assert claim["claimed_item_ids"] == [3]
    assert claim["blocked_item_ids"] == []

    trace = exports.delivery_trace(export["id"])
    assert trace["batch_id"] == batch
    assert trace["signed_count"] == 3
    assert trace["explicit_failed_item_ids"] == [3]
    assert trace["waiting_verification_item_ids"] == []
    assert len(trace["redelivery_requests"]) == 1
    assert len(trace["original_receipts"]) >= 4

    row = exports.get_entry(export["id"])
    assert "已签收3/4" in row["签收结果"]
    assert row["可补投范围"] == "3"
    assert application["request_no"] in row["补投申请"]


def test_duplicate_receipt_does_not_double_count_signature():
    batch = "TEST-DUPLICATE-RECEIPT"
    _setup(batch)
    first = delivery.record_receipt(
        batch_id=batch, item_id=1, status="signed", receipt_id="DUP-RCPT-1")
    second = delivery.record_receipt(
        batch_id=batch, item_id=1, status="signed", receipt_id="DUP-RCPT-1")
    assert first.get("duplicate") is False
    assert second["duplicate"] is True
    assert second["signed_count"] == 2  # 初始两项签收，重复回执不增加第三项


def test_unknown_submission_is_not_counted_as_explicit_failure():
    batch = "TEST-UNKNOWN-NOT-FAILED"
    delivery.create_handoff(
        batch_id=batch,
        item_ids=[1, 2, 3],
        signed_items=[1],
        failed_items=[2],
        unknown_items=[3],
    )
    trace = delivery.trace_batch(batch_id=batch)
    assert trace["signed_item_ids"] == [1]
    assert trace["explicit_failed_item_ids"] == [2]
    assert trace["waiting_verification_item_ids"] == [3]
    assert trace["failed_count"] == 1
    assert trace["waiting_verification_count"] == 1


def test_only_same_batch_items_can_be_claimed():
    batch = "TEST-SAME-BATCH"
    _setup(batch)
    other = delivery.create_handoff(batch_id="OTHER-BATCH", item_ids=[9])
    claim = delivery.claim_redelivery(batch_id=batch, item_ids=[3, 9])
    assert claim["claimed_item_ids"] == [3]
    assert [item["item_id"] for item in claim["blocked_items"] if item["reason"] == "not_in_batch"] == [9]
    assert other["batch_id"] == "OTHER-BATCH"
