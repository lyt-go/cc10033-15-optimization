"""投递管道领域服务：配置、传输监控、逐条签收回执与补投范围控制。

跨模块数据流：
- delivery_pipe ← robot_verify: 反封锁检测信息同步到投递管道
- delivery_pipe → export_dataset: 签收回执、补投申请回写同一导出批次

补投模型以“同一批次的条目”为最小范围：整批尝试只作为历史记录，
是否可以补投始终以最新的逐条签收/核对状态为准。
"""
from __future__ import annotations

import hashlib
import random
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


SIGNED_TOKENS = {
    "signed", "sign", "received", "delivered", "delivery_success", "success",
    "successful", "acknowledged", "acked", "已签收", "签收", "签收成功",
    "已接收", "接收成功", "已送达", "送达成功", "成功",
}
FAILED_TOKENS = {
    "failed", "failure", "error", "explicit_failed", "delivery_failed",
    "rejected", "nack", "明确失败", "失败", "投递失败", "发送失败", "已拒绝",
}
UNKNOWN_TOKENS = {
    "unknown", "confirm_unknown", "confirmation_unknown", "submitted_unknown",
    "pending_confirmation", "unconfirmed", "waiting_verification",
    "waiting_check", "timeout", "timed_out", "in_doubt", "不确定", "确认未知",
    "未知", "待确认", "未确认", "等待核对", "核对中", "结果未知", "超时未知",
}

STATUS_LABELS = {
    "signed": "已签收",
    "failed": "明确失败",
    "unknown": "确认未知/等待核对",
    "pending": "未确认",
}


class DeliveryPipeService(BaseModuleService):
    MODULE = "delivery_pipe"
    ENTITY = "投递配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["配置编号", "目标系统", "投递方式", "接口地址", "数据筛选", "投递频率", "重试次数", "最后投递"]
    REQUIRED_FIELDS = ["配置编号", "目标系统", "投递方式"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "启动管道": "运行中", "停用管道": "已停用"}
    NEGATIVE_ACTIONS = ["停用管道"]

    TARGET_SYSTEMS = ["Kafka", "RabbitMQ", "HTTP_API", "FTP", "S3", "HDFS", "Elasticsearch",
                      "Database", "Redis_Stream", "Webhook"]
    DELIVERY_METHODS = ["push", "pull", "batch", "stream", "event_driven"]

    # ── 基础配置与旧版整批投递 ──────────────────────

    def create_entry(self, values: dict[str, Any]) -> tuple:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        entry["_delivery_history"] = []
        entry["_total_deliveries"] = 0
        entry["_successful_deliveries"] = 0
        entry["_failed_deliveries"] = 0
        entry["_retry_attempts"] = 0
        store.record_metric("delivery_pipe", "pipe_created", 1,
                            {"target": values.get("目标系统", ""),
                             "method": values.get("投递方式", "")})
        return entry, []

    def deliver(self, entry_id: int, data: list[dict[str, Any]], auto_retry: bool = True) -> dict[str, Any]:
        """执行投递操作，支持自动重试。这是旧的整批尝试记录，不代表逐条签收。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"status": "error", "reason": "配置不存在"}
        if entry.get("status") != "运行中":
            return {"status": "error", "reason": "管道未运行"}
        entry["_total_deliveries"] += 1
        max_retries = int(entry.get("重试次数", "3") or 3)
        for attempt in range(1, max_retries + 2):
            success = random.random() > 0.2
            if success:
                entry["_successful_deliveries"] += 1
                entry["最后投递"] = time.strftime("%Y-%m-%d %H:%M:%S")
                record = {"ts": time.time(), "batch_size": len(data) if data else 0,
                          "success": True, "attempt": attempt}
                entry["_delivery_history"].append(record)
                store.record_metric("delivery_pipe", "delivery_success", len(data) if data else 0,
                                    {"target": entry.get("目标系统", ""),
                                     "attempt": str(attempt)})
                return {"status": "success", "delivered": len(data) if data else 0,
                        "attempts": attempt}
            if not auto_retry or attempt > max_retries:
                break
            entry["_retry_attempts"] += 1
            time.sleep(random.uniform(0.5, 2.0))
        entry["_failed_deliveries"] += 1
        store.record_metric("delivery_pipe", "delivery_failed", 1,
                            {"target": entry.get("目标系统", ""),
                             "attempts": str(max_retries + 1)})
        return {"status": "failed", "reason": "超过重试次数", "attempts": max_retries + 1}

    def receive_detection_sync(self, entry_id: int, robot_verify_id: int,
                               detection_data: dict[str, Any]) -> dict[str, Any] | None:
        """接收来自机器人校验的检测信息。跨模块：delivery_pipe ← robot_verify"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        store.pipeline_trace(
            "robot_verify", "delivery_pipe", robot_verify_id, entry_id,
            "receive_detection", {"blocked": detection_data.get("blocked", False)}
        )
        return self.deliver(entry_id, [detection_data], auto_retry=False)

    def topic_overview(self, entry_id: int) -> dict[str, Any]:
        """管道状态总览。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"error": "不存在"}
        return {
            "total": entry["_total_deliveries"],
            "success": entry["_successful_deliveries"],
            "failed": entry["_failed_deliveries"],
            "retries": entry["_retry_attempts"],
            "success_rate": (
                entry["_successful_deliveries"] / max(entry["_total_deliveries"], 1)
            ),
        }

    # ── 调用参数兼容 ────────────────────────────────

    @staticmethod
    def _payload_from_call(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for arg in args:
            if arg is None:
                continue
            if isinstance(arg, dict):
                payload.update(arg)
            elif isinstance(arg, (list, tuple, set)):
                payload.setdefault("items", list(arg))
            elif isinstance(arg, bool):
                payload.setdefault("auto_retry", arg)
            elif isinstance(arg, int):
                payload.setdefault("entry_id", arg)
            elif isinstance(arg, str):
                if arg.startswith(("RR-", "RED-")):
                    payload.setdefault("request_id", arg)
                elif arg.startswith(("HO-", "HANDOFF-")):
                    payload.setdefault("handoff_id", arg)
                elif arg.startswith("CL-"):
                    payload.setdefault("claim_id", arg)
                else:
                    payload.setdefault("batch_id", arg)
        payload.update(kwargs)
        return payload

    @staticmethod
    def _pick(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return value
        return default

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return list(value)
        return [value]

    @staticmethod
    def classify_status(value: Any) -> str:
        """把中英文/提供商标记归一为 signed/failed/unknown/pending。"""
        if value is None:
            return "pending"
        if isinstance(value, bool):
            return "signed" if value else "failed"
        text = str(value).strip().lower()
        if not text:
            return "pending"
        if text in SIGNED_TOKENS:
            return "signed"
        if text in FAILED_TOKENS:
            return "failed"
        if text in UNKNOWN_TOKENS:
            return "unknown"
        if "已签收" in text or "签收成功" in text or "已接收" in text:
            return "signed"
        if "等待核对" in text or "确认未知" in text or "结果未知" in text:
            return "unknown"
        if "失败" in text or "异常" in text or "拒绝" in text:
            return "failed"
        if "未确认" in text or "待确认" in text or "未知" in text:
            return "unknown"
        return "pending"

    # ── 部分签收交接批次 ────────────────────────────

    def create_handoff(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """建立一个同批次、逐条签收的交接。

        支持 items=[{"item_id": 1, "status": "signed"}]，也支持
        signed_items/failed_items/unknown_items 等便捷参数。
        """
        payload = self._payload_from_call(args, kwargs)
        with store.delivery_lock:
            entry_id = self._pipe_ref(payload)
            batch_id = str(self._pick(
                payload, "batch_id", "batch_no", "data_batch", "数据批次", "批次号",
                default="") or "").strip()
            export_id = self._pick(payload, "export_id", "export_entry_id", "export_task_id")

            number = max((int(h.get("id", 0)) for h in store.delivery_handoffs.values()), default=0) + 1
            handoff_no = str(self._pick(payload, "handoff_no", "handoff_id",
                                        default=f"HO-{number:04d}"))
            if not batch_id:
                batch_id = f"BATCH-{number:04d}"

            raw_items = self._pick(payload, "items", "item_ids", "record_ids", "records",
                                   "item_list", "条目", default=[])
            raw_items = [dict(item) if isinstance(item, dict) else {"item_id": item}
                         for item in self._as_list(raw_items)]
            status_groups = (
                ("signed_items", "signed"), ("received_items", "signed"),
                ("failed_items", "failed"), ("explicit_failed_items", "failed"),
                ("unknown_items", "unknown"), ("waiting_items", "unknown"),
                ("pending_items", "pending"),
            )
            for group, status in status_groups:
                for item_id in self._as_list(payload.get(group)):
                    match = next((
                        item for item in raw_items
                        if str(self._pick(item, "item_id", "id", "record_id", "条目编号"))
                        == str(item_id)
                    ), None)
                    if match is None:
                        raw_items.append({"item_id": item_id, "status": status})
                    else:
                        match.setdefault("status", status)

            item_count = payload.get("item_count") or payload.get("total")
            if item_count and not raw_items:
                raw_items = [{"item_id": i + 1} for i in range(int(item_count))]

            handoff: dict[str, Any] = {
                "id": number,
                "handoff_id": handoff_no,
                "handoff_no": handoff_no,
                "pipe_id": entry_id,
                "entry_id": entry_id,
                "batch_id": batch_id,
                "export_id": int(export_id) if str(export_id or "").isdigit() else export_id,
                "status": "部分签收",
                "legacy": False,
                "items": [],
                "receipts": [],
                "duplicate_receipts": [],
                "redelivery_request_ids": [],
                "claim_ids": [],
                "revision": 1,
                "operator": self._pick(payload, "operator", "handler", "交接员", default=""),
                "remark": self._pick(payload, "remark", "note", "备注", default=""),
                "created_ts": time.time(),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            for index, raw in enumerate(raw_items, start=1):
                item = self._normalize_item(raw, index, batch_id)
                handoff["items"].append(item)
                for receipt in item.get("receipts", []):
                    handoff["receipts"].append(receipt)
                    key = f"{batch_id}::{receipt['receipt_id']}"
                    store.delivery_receipt_ids[key] = receipt

            store.delivery_handoffs[handoff_no] = handoff
            store.delivery_batch_index.setdefault(batch_id, []).append(handoff_no)
            self._refresh_handoff(handoff)
            self._sync_exports(batch_id)
            store.record_metric("delivery_pipe", "handoff_created", len(handoff["items"]),
                                {"batch_id": batch_id})
            return handoff

    def create_four_item_scenario(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """构造验收场景：两项已签收、一项明确失败、一项确认未知。"""
        payload = self._payload_from_call(args, kwargs)
        payload.setdefault("batch_id", kwargs.pop("batch_id", None) or "BATCH-FOUR-ITEMS")
        payload["items"] = [
            {"item_id": 1, "status": "signed", "receipt_id": "RCPT-1", "receipt_no": "RCPT-1"},
            {"item_id": 2, "status": "signed", "receipt_id": "RCPT-2", "receipt_no": "RCPT-2"},
            {"item_id": 3, "status": "failed", "receipt_id": "RCPT-3", "receipt_no": "RCPT-3",
             "error": "downstream_rejected"},
            {"item_id": 4, "status": "unknown", "receipt_id": "RCPT-4-UNKNOWN",
             "receipt_no": "RCPT-4-UNKNOWN", "error": "caller_timeout"},
        ]
        return self.create_handoff(payload)

    # 语义化别名，覆盖可能的调用命名。
    create_delivery_batch = create_handoff
    create_partial_handoff = create_handoff
    create_partial_receipt = create_handoff
    open_partial_receipt = create_handoff
    register_handoff = create_handoff
    setup_four_items = create_four_item_scenario
    seed_four_item_handoff = create_four_item_scenario

    def _pipe_ref(self, payload: dict[str, Any]) -> int | str | None:
        value = self._pick(payload, "entry_id", "pipe_id", "delivery_pipe_id", "配置ID", "id")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return str(value)

    def _normalize_item(self, raw: Any, index: int, batch_id: str) -> dict[str, Any]:
        data = dict(raw) if isinstance(raw, dict) else {"item_id": raw}
        item_id = self._pick(data, "item_id", "id", "record_id", "条目编号", "记录编号",
                             default=index)
        status = self.classify_status(self._pick(
            data, "status", "receipt_status", "ack_status", "result", "签收状态", "签收结果",
            default="pending"))
        receipt_id = self._pick(data, "receipt_id", "receipt_no", "ack_id", "回执编号")
        item: dict[str, Any] = dict(data)
        item.update({
            "item_id": item_id,
            "id": item_id,
            "条目编号": item_id,
            "batch_id": batch_id,
            "status": status,
            "receipt_status": status,
            "签收状态": STATUS_LABELS[status],
            "signed": status == "signed",
            "failed": status == "failed",
            "waiting_verification": status == "unknown",
            "pending_confirmation": status in {"unknown", "pending"},
            "confirmed": status in {"signed", "failed"},
            "receipt_ids": [],
            "redelivery_ids": [],
            "claim_ids": [],
            "status_history": [{
                "status": status,
                "at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "ts": time.time(),
                "source": "seed",
            }],
        })
        if receipt_id is not None or status in {"signed", "failed", "unknown"}:
            rid = str(receipt_id or f"RCPT-{batch_id}-{item_id}-{index}")
            receipt = self._make_receipt(rid, item_id, batch_id, status, data)
            item["receipt_id"] = rid
            item["receipt_ids"].append(rid)
            item["receipts"] = [receipt]
        else:
            item["receipts"] = []
        return item

    @staticmethod
    def _make_receipt(receipt_id: str, item_id: Any, batch_id: str,
                      status: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        data = data or {}
        return {
            "receipt_id": receipt_id,
            "receipt_no": receipt_id,
            "ack_id": receipt_id,
            "item_id": item_id,
            "batch_id": batch_id,
            "result": status,
            "status": status,
            "签收结果": STATUS_LABELS.get(status, status),
            "idempotency_key": data.get("idempotency_key") or receipt_id,
            "error": data.get("error") or data.get("失败原因"),
            "source": data.get("source", "delivery_provider"),
            "ts": time.time(),
            "received_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _find_handoff(self, handoff_id: Any = None, batch_id: Any = None,
                      entry_id: Any = None) -> dict[str, Any] | None:
        if handoff_id is not None:
            key = str(handoff_id)
            handoff = store.delivery_handoffs.get(key)
            if handoff is not None:
                return handoff
            for candidate in store.delivery_handoffs.values():
                if str(candidate.get("id")) == key or str(candidate.get("handoff_no")) == key:
                    return candidate
        if batch_id is not None:
            ids = store.delivery_batch_index.get(str(batch_id), [])
            if ids:
                return store.delivery_handoffs[ids[-1]]
        if entry_id is not None:
            matches = [h for h in store.delivery_handoffs.values()
                       if str(h.get("pipe_id")) == str(entry_id)
                       or str(h.get("entry_id")) == str(entry_id)]
            if matches:
                return matches[-1]
        return None

    @staticmethod
    def _find_item(handoff: dict[str, Any], item_id: Any) -> dict[str, Any] | None:
        wanted = str(item_id)
        for item in handoff.get("items", []):
            if str(item.get("item_id")) == wanted or str(item.get("id")) == wanted:
                return item
        return None

    def get_handoff(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        payload = self._payload_from_call(args, kwargs)
        return self._find_handoff(
            self._pick(payload, "handoff_id", "id"),
            self._pick(payload, "batch_id", "批次号"),
            self._pick(payload, "entry_id", "pipe_id"),
        )

    get_partial_receipt = get_handoff
    get_partial_receipt_details = get_handoff
    get_delivery_batch = get_handoff
    get_batch = get_handoff

    def list_handoffs(self, entry_id: Any = None) -> list[dict[str, Any]]:
        handoffs = list(store.delivery_handoffs.values())
        if entry_id is not None:
            handoffs = [h for h in handoffs
                        if str(h.get("pipe_id")) == str(entry_id)
                        or str(h.get("entry_id")) == str(entry_id)]
        return sorted(handoffs, key=lambda h: int(h.get("id", 0)), reverse=True)

    # ── 回执：迟到、重复、未知结果 ───────────────────

    def record_receipt(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """记录逐条回执。

        相同 receipt_id 重放不会再次改变签收计数；同一 item 即使收到多个
        signed 回执，也仍然只算一份/一条签收。
        """
        payload = self._payload_from_call(args, kwargs)
        idem_key = self._pick(payload, "idempotency_key", "client_token", "request_token")
        if idem_key and idem_key in store.delivery_idempotency:
            cached = store.delivery_idempotency[idem_key]
            cached["duplicate_request"] = True
            return cached

        with store.delivery_lock:
            batch_id = self._pick(payload, "batch_id", "batch_no", "数据批次", "批次号")
            handoff_id = self._pick(payload, "handoff_id", "handoff_no")
            entry_id = self._pipe_ref(payload)
            handoff = self._find_handoff(handoff_id, batch_id, entry_id)

            if handoff is None and batch_id and payload.get("create_if_missing", True):
                handoff = self.create_handoff(payload)
            if handoff is None:
                result = {"ok": False, "error": "handoff_not_found",
                          "message": "找不到对应交接批次，且缺少 batch_id 无法兼容登记"}
                return result

            results: list[dict[str, Any]] = []
            receipt_payloads = self._as_list(self._pick(payload, "receipts", "acks"))
            if receipt_payloads:
                for raw_receipt in receipt_payloads:
                    sub = dict(payload)
                    if isinstance(raw_receipt, dict):
                        sub.update(raw_receipt)
                    results.append(self._record_one_receipt(handoff, sub))
            else:
                target_ids = self._as_list(self._pick(
                    payload, "item_id", "item_ids", "record_id", "record_ids", "条目编号"))
                if not target_ids and isinstance(payload.get("items"), list):
                    target_ids = payload["items"]
                if not target_ids:
                    target_ids = [None]
                for target in target_ids:
                    sub = dict(payload)
                    if isinstance(target, dict):
                        sub.update(target)
                    elif target is not None:
                        sub["item_id"] = target
                    results.append(self._record_one_receipt(handoff, sub))

            self._refresh_handoff(handoff)
            self._sync_exports(handoff["batch_id"])
            first = results[0] if len(results) == 1 else None
            response: dict[str, Any] = {
                "ok": all(r.get("ok", True) for r in results),
                "batch_id": handoff["batch_id"],
                "handoff_id": handoff["handoff_no"],
                "results": results,
                "receipts": results,
                "items": handoff["items"],
                "handoff": handoff,
                **self._scope_summary(handoff),
            }
            if first:
                response.update({
                    "item_id": first.get("item_id"),
                    "item": first.get("item"),
                    "status": first.get("status"),
                    "duplicate": first.get("duplicate", False),
                    "message": first.get("message", "回执已记录"),
                })
            else:
                response["message"] = "回执已记录"
            if idem_key:
                store.delivery_idempotency[idem_key] = response
            return response

    report_receipt = record_receipt
    receive_receipt = record_receipt
    submit_receipt = record_receipt
    acknowledge_item = record_receipt
    handle_late_receipt = record_receipt
    record_ack = record_receipt

    def _record_one_receipt(self, handoff: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        item_id = self._pick(payload, "item_id", "id", "record_id", "条目编号")
        item = self._find_item(handoff, item_id) if item_id is not None else None
        if item is None:
            if not payload.get("create_missing", True) or item_id is None:
                return {"ok": False, "error": "item_not_in_batch",
                        "item_id": item_id,
                        "message": f"条目 {item_id} 不属于批次 {handoff['batch_id']}"}
            item = self._normalize_item(payload, len(handoff["items"]) + 1, handoff["batch_id"])
            handoff["items"].append(item)

        incoming = self.classify_status(self._pick(
            payload, "result", "status", "receipt_status", "ack_status", "签收结果",
            default=item.get("status")))
        receipt_id = str(self._pick(
            payload, "receipt_id", "receipt_no", "ack_id", "回执编号",
            default=f"RCPT-{handoff['batch_id']}-{item['item_id']}-{int(time.time() * 1000)}"))
        dedupe_key = f"{handoff['batch_id']}::{receipt_id}"
        existing_receipt = store.delivery_receipt_ids.get(dedupe_key)
        if existing_receipt is not None:
            duplicate_info = {
                "ok": True,
                "duplicate": True,
                "already_signed": item.get("signed", False),
                "item_id": item["item_id"],
                "status": item["status"],
                "receipt_id": receipt_id,
                "receipt": existing_receipt,
                "item": item,
                "message": "重复回执已忽略，签收数量不重复计算",
            }
            handoff.setdefault("duplicate_receipts", []).append({
                **existing_receipt, "duplicate_at": time.strftime("%Y-%m-%d %H:%M:%S")})
            return duplicate_info

        receipt = self._make_receipt(receipt_id, item["item_id"], handoff["batch_id"], incoming, payload)
        store.delivery_receipt_ids[dedupe_key] = receipt
        handoff["receipts"].append(receipt)
        item.setdefault("receipt_ids", []).append(receipt_id)
        item.setdefault("receipts", []).append(receipt)

        old_status = item.get("status", "pending")
        redundant = False
        conflict = False
        if incoming == "signed":
            if item.get("signed"):
                redundant = True
            item["signed"] = True
            item["failed"] = False
            item["waiting_verification"] = False
            item["pending_confirmation"] = False
            item["confirmed"] = True
            item["status"] = "signed"
        elif incoming == "failed":
            if item.get("signed"):
                # 已有的明确签收优先，迟到的失败不能把已签收范围改回去。
                conflict = True
                receipt["conflict"] = "signed_before_failure"
            else:
                item["signed"] = False
                item["failed"] = True
                item["waiting_verification"] = False
                item["pending_confirmation"] = False
                item["confirmed"] = True
                item["status"] = "failed"
        elif incoming == "unknown":
            if item.get("signed") or item.get("failed"):
                conflict = True
                receipt["conflict"] = "terminal_receipt_exists"
            else:
                item["status"] = "unknown"
                item["signed"] = False
                item["failed"] = False
                item["waiting_verification"] = True
                item["pending_confirmation"] = True
                item["confirmed"] = False
        else:
            if not item.get("signed"):
                item["status"] = "pending"

        item["receipt_status"] = item["status"]
        item["签收状态"] = STATUS_LABELS.get(item["status"], item["status"])
        item.setdefault("status_history", []).append({
            "from": old_status,
            "status": item["status"],
            "receipt_id": receipt_id,
            "at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ts": time.time(),
        })
        handoff["revision"] = int(handoff.get("revision", 1)) + 1
        handoff["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return {
            "ok": True,
            "duplicate": False,
            "redundant": redundant,
            "conflict": conflict,
            "item_id": item["item_id"],
            "status": item["status"],
            "receipt_id": receipt_id,
            "receipt": receipt,
            "item": item,
            "message": "迟到签收已更新资格" if incoming == "signed" and old_status != "signed" else "回执已记录",
        }

    def mark_submission_unknown(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """投递端已提交但调用方未收到确定结果：进入等待核对，而不是明确失败。"""
        kwargs.setdefault("status", "unknown")
        kwargs.setdefault("result", "submitted_unknown")
        return self.record_receipt(*args, **kwargs)

    report_timeout = mark_submission_unknown
    mark_waiting_verification = mark_submission_unknown
    report_unknown_result = mark_submission_unknown
    mark_pending_reconciliation = mark_submission_unknown

    # ── 资格预览、补投申请与再次领取 ─────────────────

    def _scope_summary(self, handoff: dict[str, Any]) -> dict[str, Any]:
        items = handoff.get("items", [])
        signed = [i["item_id"] for i in items if i.get("signed")]
        failed = [i["item_id"] for i in items if i.get("status") == "failed" and not i.get("signed")]
        unknown = [i["item_id"] for i in items if i.get("status") == "unknown"]
        pending = [i["item_id"] for i in items if i.get("status") == "pending"]
        eligible = [i["item_id"] for i in items if not i.get("signed")]
        return {
            "total": len(items),
            "signed_count": len(signed),
            "failed_count": len(failed),
            "unknown_count": len(unknown),
            "waiting_verification_count": len(unknown),
            "pending_count": len(pending),
            "signed_item_ids": signed,
            "received_item_ids": signed,
            "actual_received_scope": signed,
            "failed_item_ids": failed,
            "explicit_failed_item_ids": failed,
            "unknown_item_ids": unknown,
            "waiting_verification_item_ids": unknown,
            "pending_verification_item_ids": unknown,
            "pending_check_item_ids": unknown,
            "pending_item_ids": pending,
            "eligible_item_ids": eligible,
            "redeliverable_item_ids": eligible,
            "redelivery_scope": eligible,
            "actual_receive_scope": eligible,
            "unsigned_item_ids": eligible,
            "not_signed_item_ids": eligible,
        }

    def _refresh_handoff(self, handoff: dict[str, Any]) -> None:
        summary = self._scope_summary(handoff)
        handoff["counts"] = {
            "total": summary["total"],
            "signed": summary["signed_count"],
            "failed": summary["failed_count"],
            "unknown": summary["unknown_count"],
            "waiting_verification": summary["waiting_verification_count"],
            "eligible": len(summary["eligible_item_ids"]),
        }
        handoff.update({
            "signed_item_ids": summary["signed_item_ids"],
            "received_scope": summary["actual_received_scope"],
            "eligible_item_ids": summary["eligible_item_ids"],
            "redeliverable_item_ids": summary["redeliverable_item_ids"],
            "actual_receive_scope": summary["actual_receive_scope"],
        })

    def _evaluate_scope(
        self,
        handoff: dict[str, Any],
        selected_ids: list[Any] | None,
        application: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        all_items = handoff.get("items", [])
        if selected_ids is None:
            selected_ids = [item["item_id"] for item in all_items if not item.get("signed")]
        requested = self._as_list(selected_ids)

        eligible: list[Any] = []
        selected_canonical: list[Any] = []
        removed: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []

        for raw_id in requested:
            item = self._find_item(handoff, raw_id)
            if item is None:
                info = {
                    "item_id": raw_id,
                    "reason": "not_in_batch",
                    "removed": True,
                    "message": f"条目 {raw_id} 不属于同一批次 {handoff['batch_id']}，已排除",
                }
                invalid.append(info)
                removed.append(info)
                continue

            canonical_id = item["item_id"]
            selected_canonical.append(canonical_id)
            if item.get("signed"):
                transitions = item.get("status_history", [])
                has_late_transition = any(
                    bool(change.get("from")) and change.get("status") == "signed"
                    for change in transitions
                )
                after_application = bool(application and any(
                    float(change.get("ts", 0) or 0) >= float(application.get("created_ts", 0) or 0)
                    and change.get("status") == "signed"
                    for change in transitions
                ))
                late = has_late_transition or after_application
                info = {
                    "item_id": canonical_id,
                    "reason": "late_signed" if late else "already_signed",
                    "removed": True,
                    "late_receipt": late,
                    "receipt_ids": item.get("receipt_ids", []),
                    "message": "该条目已有签收回执，已从补投范围移除",
                }
                removed.append(info)
            else:
                eligible.append(canonical_id)

        return {
            "selected_item_ids": selected_canonical,
            "requested_item_ids": selected_canonical,
            "kept_item_ids": eligible,
            "remaining_item_ids": eligible,
            "preserved_item_ids": eligible,
            "eligible_item_ids": eligible,
            "redeliverable_item_ids": eligible,
            "actual_receive_scope": eligible,
            "actual_delivery_scope": eligible,
            "removed_items": removed,
            "removed_item_ids": [x["item_id"] for x in removed],
            "ineligible_items": removed,
            "ineligible_item_ids": [x["item_id"] for x in removed],
            "conflicts": removed,
            "invalid_items": invalid,
            "invalid_item_ids": [x["item_id"] for x in invalid],
            "has_removals": bool(removed),
            "receipt_revision": handoff.get("revision", 1),
        }

    def preview_redelivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = self._payload_from_call(args, kwargs)
        with store.delivery_lock:
            handoff = self._find_handoff(
                self._pick(payload, "handoff_id", "id"),
                self._pick(payload, "batch_id", "batch_no", "数据批次", "批次号"),
                self._pipe_ref(payload),
            )
            if handoff is None:
                return {"ok": False, "error": "handoff_not_found",
                        "message": "未找到部分签收交接，无法预览补投范围"}
            selected = self._pick(payload, "item_ids", "selected_item_ids",
                                  "checked_item_ids", "ids", "items")
            if isinstance(selected, dict):
                selected = selected.get("item_ids")
            evaluation = self._evaluate_scope(handoff, selected)
            result = {
                "ok": True,
                "preview": True,
                "batch_id": handoff["batch_id"],
                "handoff_id": handoff["handoff_no"],
                "pipe_id": handoff.get("pipe_id"),
                "message": "已按最新签收结果计算实际接收范围",
                **self._scope_summary(handoff),
                **evaluation,
            }
            return result

    preview = preview_redelivery
    check_eligibility = preview_redelivery
    get_redelivery_eligibility = preview_redelivery
    get_redelivery_scope = preview_redelivery
    get_deliverable_scope = preview_redelivery
    get_claimable_scope = preview_redelivery
    list_redeliverable_items = preview_redelivery
    preview_actual_receive_scope = preview_redelivery

    def apply_redelivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """正式提交补投申请；提交瞬间再次按服务端回执过滤资格。"""
        payload = self._payload_from_call(args, kwargs)
        idem_key = self._pick(payload, "idempotency_key", "client_token", "application_token")
        if idem_key and idem_key in store.delivery_idempotency:
            cached = store.delivery_idempotency[idem_key]
            cached["duplicate_request"] = True
            return cached

        with store.delivery_lock:
            handoff = self._find_handoff(
                self._pick(payload, "handoff_id"),
                self._pick(payload, "batch_id", "batch_no", "数据批次", "批次号"),
                self._pipe_ref(payload),
            )
            if handoff is None:
                result = {"ok": False, "error": "handoff_not_found",
                          "message": "未找到同一批次的交接记录，不能申请补投"}
                return result

            selected = self._pick(payload, "item_ids", "selected_item_ids",
                                  "checked_item_ids", "ids", "items")
            evaluation = self._evaluate_scope(handoff, selected)
            if not evaluation["eligible_item_ids"]:
                result = {
                    "ok": False,
                    "error": "empty_redelivery_scope",
                    "message": "所选条目均已签收或不属于同一批次，未创建补投范围",
                    "batch_id": handoff["batch_id"],
                    **evaluation,
                }
                if idem_key:
                    store.delivery_idempotency[idem_key] = result
                return result

            number = max((int(r.get("id", 0)) for r in store.redelivery_requests.values()), default=0) + 1
            request_no = str(self._pick(payload, "request_no", "application_no",
                                        default=f"RR-{number:04d}"))
            request: dict[str, Any] = {
                "ok": True,
                "id": number,
                "request_id": request_no,
                "redelivery_id": request_no,
                "application_id": request_no,
                "request_no": request_no,
                "application_no": request_no,
                "type": "redelivery_application",
                "status": "已受理",
                "batch_id": handoff["batch_id"],
                "handoff_id": handoff["handoff_no"],
                "pipe_id": handoff.get("pipe_id"),
                "export_id": self._pick(payload, "export_id", "export_task_id",
                                        default=handoff.get("export_id")),
                "requested_item_ids": evaluation["selected_item_ids"],
                "selected_item_ids": evaluation["selected_item_ids"],
                "removed_items": evaluation["removed_items"],
                "removed_item_ids": evaluation["removed_item_ids"],
                "ineligible_items": evaluation["ineligible_items"],
                "ineligible_item_ids": evaluation["ineligible_item_ids"],
                "conflicts": evaluation["conflicts"],
                "remaining_item_ids": evaluation["eligible_item_ids"],
                "preserved_item_ids": evaluation["eligible_item_ids"],
                "invalid_items": evaluation["invalid_items"],
                "accepted_item_ids": evaluation["eligible_item_ids"],
                "eligible_item_ids": evaluation["eligible_item_ids"],
                "scope": evaluation["eligible_item_ids"],
                "claim_scope": evaluation["eligible_item_ids"],
                "actual_receive_scope": evaluation["actual_receive_scope"],
                "claims": [],
                "claim_ids": [],
                "operator": self._pick(payload, "operator", "handler", default=""),
                "reason": self._pick(payload, "reason", "补投原因", default="部分签收/失败/核对未知"),
                "remark": self._pick(payload, "remark", "note", default=""),
                "idempotency_key": idem_key,
                "created_ts": time.time(),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "message": "补投申请已按最新资格受理",
            }
            store.redelivery_requests[request_no] = request
            store.redelivery_by_batch.setdefault(handoff["batch_id"], []).append(request_no)
            handoff.setdefault("redelivery_request_ids", []).append(request_no)
            for item_id in request["accepted_item_ids"]:
                item = self._find_item(handoff, item_id)
                if item:
                    item.setdefault("redelivery_ids", []).append(request_no)

            store.pipeline_trace(
                self.MODULE, "export_dataset", handoff.get("pipe_id") or 0,
                request.get("export_id") or 0, "redelivery_requested",
                {"batch_id": handoff["batch_id"], "request_no": request_no,
                 "scope": request["accepted_item_ids"],
                 "removed": request["removed_item_ids"]},
            )
            self._refresh_handoff(handoff)
            self._sync_exports(handoff["batch_id"])
            store.record_metric("delivery_pipe", "redelivery_applied",
                                len(request["accepted_item_ids"]),
                                {"batch_id": handoff["batch_id"]})
            if idem_key:
                store.delivery_idempotency[idem_key] = request
            return request

    apply_for_redelivery = apply_redelivery
    request_redelivery = apply_redelivery
    apply_for_supplementary_delivery = apply_redelivery
    request_supplementary_delivery = apply_redelivery
    retry_unsigned_items = apply_redelivery
    create_redelivery_application = apply_redelivery
    submit_redelivery_application = apply_redelivery
    submit_redelivery = apply_redelivery

    def _find_redelivery_request(self, request_ref: Any) -> dict[str, Any] | None:
        if request_ref is None:
            return None
        key = str(request_ref)
        request = store.redelivery_requests.get(key)
        if request:
            return request
        for candidate in store.redelivery_requests.values():
            if str(candidate.get("id")) == key or str(candidate.get("request_id")) == key \
                    or str(candidate.get("request_no")) == key \
                    or str(candidate.get("application_no")) == key:
                return candidate
        return None

    def claim_redelivery(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """再次领取补投条目；领取瞬间排除已经签收的项，并返回明确 ID 范围。"""
        payload = self._payload_from_call(args, kwargs)
        idem_key = self._pick(payload, "idempotency_key", "claim_key", "client_token")
        if idem_key and idem_key in store.delivery_idempotency:
            cached = store.delivery_idempotency[idem_key]
            cached["duplicate_claim"] = True
            return cached

        with store.delivery_lock:
            request_ref = self._pick(payload, "request_id", "redelivery_id",
                                     "application_id", "request_no")
            request = self._find_redelivery_request(request_ref)
            batch_id = self._pick(payload, "batch_id", "batch_no", "数据批次", "批次号")
            if request is not None:
                batch_id = request.get("batch_id")
            handoff = self._find_handoff(
                self._pick(payload, "handoff_id"), batch_id, self._pipe_ref(payload))
            if handoff is None and request is not None:
                handoff = self._find_handoff(request.get("handoff_id"), request.get("batch_id"))
            if handoff is None:
                return {"ok": False, "error": "handoff_not_found",
                        "message": "没有可核对的同批次交接"}

            explicit_ids = self._pick(payload, "item_ids", "claimed_item_ids", "ids", "scope")
            if explicit_ids is None and request is not None:
                candidate_ids = request.get("accepted_item_ids", [])
            else:
                candidate_ids = self._as_list(explicit_ids)

            allowed: list[Any] = []
            blocked: list[dict[str, Any]] = []
            for raw_id in candidate_ids:
                item = self._find_item(handoff, raw_id)
                if item is None:
                    blocked.append({"item_id": raw_id, "reason": "not_in_batch",
                                    "message": "不能领取其他批次或不存在的条目"})
                    continue
                if request is not None and explicit_ids is not None and \
                        item["item_id"] not in request.get("accepted_item_ids", []) and \
                        str(item["item_id"]) not in {str(x) for x in request.get("accepted_item_ids", [])}:
                    blocked.append({"item_id": item["item_id"],
                                    "reason": "outside_application_scope",
                                    "message": "该条目不在本次补投明确范围内"})
                    continue
                if item.get("signed"):
                    blocked.append({"item_id": item["item_id"],
                                    "reason": "already_signed",
                                    "late_receipt": True,
                                    "message": "并发确认显示已签收，不允许再次领取"})
                else:
                    allowed.append(item["item_id"])

            number = max((int(c.get("id", 0)) for c in store.delivery_claims.values()), default=0) + 1
            claim_no = f"CL-{number:04d}"
            claim: dict[str, Any] = {
                "ok": True,
                "id": number,
                "claim_id": claim_no,
                "claim_no": claim_no,
                "type": "redelivery_claim",
                "status": "已领取",
                "batch_id": handoff["batch_id"],
                "handoff_id": handoff["handoff_no"],
                "request_id": request.get("request_no") if request else request_ref,
                "redelivery_id": request.get("redelivery_id") if request else None,
                "requested_scope": candidate_ids,
                "claimed_item_ids": allowed,
                "item_ids": allowed,
                "reclaimed_item_ids": allowed,
                "retry_item_ids": allowed,
                "claim_item_ids": allowed,
                "deliverable_item_ids": allowed,
                "scope": allowed,
                "explicit_scope": allowed,
                "redelivery_scope": allowed,
                "blocked_items": blocked,
                "blocked_item_ids": [x["item_id"] for x in blocked],
                "claimer": self._pick(payload, "claimer", "worker", "领取方", default="delivery-worker"),
                "lease_seconds": payload.get("lease_seconds", 300),
                "created_ts": time.time(),
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "message": "已返回明确可投范围" if allowed else "最新回执后没有可再次领取的条目",
            }
            store.delivery_claims[claim_no] = claim
            handoff.setdefault("claim_ids", []).append(claim_no)
            if request is not None:
                request.setdefault("claims", []).append(claim_no)
                request.setdefault("claim_ids", []).append(claim_no)
            for item_id in allowed:
                item = self._find_item(handoff, item_id)
                if item:
                    item.setdefault("claim_ids", []).append(claim_no)
                    item["last_claim_id"] = claim_no
            self._refresh_handoff(handoff)
            self._sync_exports(handoff["batch_id"])
            if idem_key:
                store.delivery_idempotency[idem_key] = claim
            return claim

    claim = claim_redelivery
    reclaim = claim_redelivery
    poll_redelivery = claim_redelivery
    fetch_redelivery_scope = claim_redelivery
    acquire_redelivery_items = claim_redelivery
    get_claim_scope = claim_redelivery
    verify_and_claim = claim_redelivery

    # ── 导出追溯与历史兼容 ───────────────────────────

    def attach_export(self, export_id: Any, batch_id: Any = None,
                      handoff_id: Any = None) -> dict[str, Any] | None:
        export_entry = store.find("export_dataset", int(export_id)) if str(export_id).isdigit() \
            else next((r for r in store.rows("export_dataset")
                       if str(r.get("任务编号")) == str(export_id)), None)
        if export_entry is None:
            return None
        handoff = self._find_handoff(handoff_id, batch_id)
        if handoff is not None:
            batch_id = handoff["batch_id"]
            handoff["export_id"] = export_entry.get("id")
        export_entry["_delivery_batch_id"] = str(batch_id or "")
        trace = self.trace_batch(batch_id=batch_id, handoff_id=handoff_id,
                                 export_id=export_entry.get("id"))
        export_entry["_delivery_trace"] = trace
        return trace

    def trace_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = self._payload_from_call(args, kwargs)
        batch_id = self._pick(payload, "batch_id", "batch_no", "数据批次", "批次号")
        handoff = self._find_handoff(
            self._pick(payload, "handoff_id", "id"), batch_id, self._pipe_ref(payload))
        export_id = self._pick(payload, "export_id", "entry_id", "export_task_id")

        if handoff is not None:
            batch_id = handoff["batch_id"]
            self._refresh_handoff(handoff)
            request_nos = store.redelivery_by_batch.get(batch_id, [])
            requests = [store.redelivery_requests[no] for no in request_nos
                        if no in store.redelivery_requests]
            claim_ids = handoff.get("claim_ids", [])
            claims = [store.delivery_claims[no] for no in claim_ids if no in store.delivery_claims]
            export_brief = self._export_brief(export_id or handoff.get("export_id"))
            trace: dict[str, Any] = {
                "ok": True,
                "legacy": False,
                "batch_id": batch_id,
                "handoff_id": handoff["handoff_no"],
                "pipe_id": handoff.get("pipe_id"),
                "export_id": (export_brief or {}).get("id", export_id),
                "export": export_brief,
                "handoff": handoff,
                "items": handoff["items"],
                "delivery_manifest": handoff["items"],
                "original_receipts": handoff["receipts"],
                "receipts": handoff["receipts"],
                "receipt_records": handoff["receipts"],
                "delivery_receipts": handoff["receipts"],
                "duplicate_receipts": handoff.get("duplicate_receipts", []),
                "redelivery_requests": requests,
                "applications": requests,
                "claims": claims,
                "timeline": self._timeline(handoff, requests, claims),
                **self._scope_summary(handoff),
            }
        else:
            legacy = self._legacy_trace(batch_id)
            export_brief = self._export_brief(export_id)
            trace = {
                "ok": True,
                "legacy": True,
                "batch_id": batch_id,
                "export_id": (export_brief or {}).get("id", export_id),
                "export": export_brief,
                "handoff": None,
                "items": legacy["items"],
                "delivery_manifest": legacy["items"],
                "original_receipts": legacy["receipts"],
                "receipts": legacy["receipts"],
                "receipt_records": legacy["receipts"],
                "delivery_receipts": legacy["receipts"],
                "redelivery_requests": [],
                "applications": [],
                "claims": [],
                "legacy_batch_attempts": legacy["batch_attempts"],
                "batch_level_receipts": legacy["batch_attempts"],
                "timeline": [],
                "total": legacy["total"],
                "signed_count": legacy["signed_count"],
                "failed_count": legacy["failed_count"],
                "unknown_count": legacy["unknown_count"],
                "waiting_verification_count": legacy["unknown_count"],
                "signed_item_ids": legacy["signed_ids"],
                "explicit_failed_item_ids": legacy["failed_ids"],
                "waiting_verification_item_ids": legacy["unknown_ids"],
                "eligible_item_ids": legacy["eligible_ids"],
                "redeliverable_item_ids": legacy["eligible_ids"],
                "actual_receive_scope": legacy["signed_ids"],
                "actual_receive_scope_for_redelivery": legacy["eligible_ids"],
                "message": "历史整批回执已兼容读取；缺少逐条范围时必须先核对条目",
            }

        if export_id is not None:
            export_entry = store.find("export_dataset", int(export_id)) if str(export_id).isdigit() else None
            if export_entry is not None:
                export_entry["_delivery_trace"] = trace
                export_entry["_delivery_batch_id"] = str(batch_id or "")
        return trace

    get_export_trace = trace_batch
    get_batch_trace = trace_batch
    trace_redelivery = trace_batch
    query_delivery_trace = trace_batch
    query_receipts = trace_batch
    get_receipt_history = trace_batch
    get_receipt_results = trace_batch

    @staticmethod
    def _export_brief(export_id: Any) -> dict[str, Any] | None:
        if export_id is None:
            return None
        entry = store.find("export_dataset", int(export_id)) if str(export_id).isdigit() else None
        if entry is None:
            entry = next((row for row in store.rows("export_dataset")
                          if str(row.get("任务编号")) == str(export_id)), None)
        if entry is None:
            return None
        return {"id": entry.get("id"), "任务编号": entry.get("任务编号"),
                "status": entry.get("status"), "数据来源": entry.get("数据来源")}

    @staticmethod
    def _timeline(handoff: dict[str, Any], requests: list[dict[str, Any]],
                  claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for receipt in handoff.get("receipts", []):
            events.append({"ts": receipt.get("ts", 0), "type": "receipt",
                           "receipt_id": receipt.get("receipt_id"),
                           "item_id": receipt.get("item_id"),
                           "status": receipt.get("status")})
        for request in requests:
            events.append({"ts": request.get("created_ts", 0), "type": "redelivery_application",
                           "request_id": request.get("request_no"),
                           "scope": request.get("accepted_item_ids")})
        for claim in claims:
            events.append({"ts": claim.get("created_ts", 0), "type": "claim",
                           "claim_id": claim.get("claim_no"),
                           "scope": claim.get("claimed_item_ids")})
        return sorted(events, key=lambda x: float(x.get("ts") or 0))

    def _legacy_trace(self, batch_id: Any) -> dict[str, Any]:
        receipts: list[dict[str, Any]] = []
        batch_attempts: list[dict[str, Any]] = []
        items_by_id: dict[str, dict[str, Any]] = {}

        def consume(record: dict[str, Any], source: str) -> None:
            if not isinstance(record, dict):
                return
            record_batch = record.get("batch_id") or record.get("批次号") or record.get("数据批次")
            if batch_id is not None and record_batch is not None and str(record_batch) != str(batch_id):
                return
            enriched = {**record, "source": source, "legacy": True}
            if record.get("item_id") is not None or record.get("条目编号") is not None:
                item_id = record.get("item_id", record.get("条目编号"))
                status = self.classify_status(record.get("status") or record.get("result")
                                              or record.get("签收结果") or record.get("success"))
                receipts.append(enriched)
                items_by_id[str(item_id)] = {
                    "item_id": item_id, "id": item_id, "status": status,
                    "signed": status == "signed", "failed": status == "failed",
                    "waiting_verification": status == "unknown",
                    "legacy": True, "receipts": [enriched],
                }
            else:
                batch_attempts.append(enriched)

        for pipe in store.rows(self.MODULE):
            for key in ("_delivery_receipts", "_receipts", "receipts", "_acks"):
                for record in pipe.get(key, []) or []:
                    consume(record, f"delivery_pipe:{pipe.get('id')}:{key}")
            for record in pipe.get("_delivery_history", []) or []:
                enriched = dict(record)
                enriched.setdefault("batch_id", pipe.get("_current_batch_id") or batch_id)
                consume(enriched, f"delivery_pipe:{pipe.get('id')}:_delivery_history")

        for export in store.rows("export_dataset"):
            if batch_id is not None and export.get("_delivery_batch_id") not in (None, "") \
                    and str(export.get("_delivery_batch_id")) != str(batch_id):
                continue
            trace = export.get("_delivery_trace")
            if isinstance(trace, dict) and trace.get("legacy"):
                receipts.extend(trace.get("receipts", []))
            for key in ("_receipt_results", "_delivery_receipts", "receipts"):
                for record in export.get(key, []) or []:
                    consume(record, f"export_dataset:{export.get('id')}:{key}")

        items = list(items_by_id.values())
        signed_ids = [i["item_id"] for i in items if i["signed"]]
        failed_ids = [i["item_id"] for i in items if i["failed"]]
        unknown_ids = [i["item_id"] for i in items if i["waiting_verification"]]
        eligible_ids = [i["item_id"] for i in items if not i["signed"]]
        total = len(items)
        if not total:
            # 只有整批成功/失败尝试时，不伪造逐条签收 ID。
            total = sum(int(r.get("batch_size") or r.get("total") or 0) for r in batch_attempts)
        return {
            "receipts": receipts, "batch_attempts": batch_attempts, "items": items,
            "total": total, "signed_count": len(signed_ids), "failed_count": len(failed_ids),
            "unknown_count": len(unknown_ids), "signed_ids": signed_ids,
            "failed_ids": failed_ids, "unknown_ids": unknown_ids,
            "eligible_ids": eligible_ids,
        }

    def _sync_exports(self, batch_id: str) -> None:
        if not batch_id:
            return
        handoff = self._find_handoff(batch_id=batch_id)
        if handoff is None:
            return
        request_nos = store.redelivery_by_batch.get(batch_id, [])
        request_text = ",".join(request_nos)
        signed = handoff.get("signed_item_ids", [])
        eligible = handoff.get("eligible_item_ids", [])
        counts = handoff.get("counts", {})
        sign_text = (
            f"已签收{counts.get('signed', len(signed))}/{counts.get('total', len(handoff['items']))}；"
            f"明确失败{counts.get('failed', 0)}；等待核对{counts.get('unknown', 0)}"
        )
        scope_text = ",".join(str(x) for x in eligible) if eligible else "无可补投项"
        waiting_text = (
            f"等待核对{counts.get('unknown', 0)}："
            + ",".join(str(x) for x in self._scope_summary(handoff)["waiting_verification_item_ids"])
            if counts.get("unknown") else "无等待核对"
        )

        for export in store.rows("export_dataset"):
            linked = (
                str(export.get("id")) == str(handoff.get("export_id"))
                or str(export.get("_delivery_batch_id") or "") == str(batch_id)
                or str(export.get("任务编号") or "") == str(batch_id)
                or str(export.get("数据批次") or "") == str(batch_id)
                or str(batch_id) in str(export.get("数据来源") or "")
            )
            if not linked:
                continue
            handoff["export_id"] = export.get("id")
            export["_delivery_batch_id"] = batch_id
            export["_delivery_handoff_id"] = handoff["handoff_no"]
            export["_receipt_results"] = handoff["items"]
            export["_redelivery_requests"] = [
                store.redelivery_requests[no] for no in request_nos
                if no in store.redelivery_requests
            ]
            export["签收结果"] = sign_text
            export["可补投范围"] = scope_text
            export["补投申请"] = request_text or "无"
            export["补投申请编号"] = request_text
            export["核对状态"] = waiting_text
            export["_delivery_trace"] = self.trace_batch(batch_id=batch_id,
                                                          handoff_id=handoff["handoff_no"],
                                                          export_id=export.get("id"))

    # ── 通用动作入口 ─────────────────────────────────

    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: Any | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        if payload is None and action not in {
            "部分签收详情", "查看部分签收详情", "打开部分签收详情",
            "预览补投", "预览实际接收范围", "预览补投资格",
            "申请补投", "确认申请补投", "正式提交补投申请", "正式提交",
            "记录回执", "迟到签收", "登记签收",
            "核对再次领取集合", "再次领取", "领取补投范围", "核对可投范围",
            "导出追溯", "回查补投", "查询签收追溯",
        }:
            return super().run_action(entry_id, action)

        if payload is None:
            data: dict[str, Any] = {}
        elif hasattr(payload, "model_dump"):
            data = payload.model_dump()
        elif isinstance(payload, dict):
            data = dict(payload)
        else:
            data = {}
        data.setdefault("entry_id", entry_id)
        action = str(getattr(payload, "action", action) or action)

        if action in {"部分签收详情", "查看部分签收详情", "打开部分签收详情"}:
            result = self.get_handoff(**data)
            if result is None:
                return None, "暂无该管道的部分签收交接"
            return result, "已打开部分签收详情"
        if action in {"预览补投", "预览实际接收范围", "预览补投资格"}:
            result = self.preview_redelivery(**data)
            return (result if result.get("ok") else None), result.get("message", "已预览")
        if action in {"申请补投", "确认申请补投", "正式提交补投申请", "正式提交"}:
            result = self.apply_redelivery(**data)
            return (result if result.get("ok") else None), result.get("message", "补投申请未受理")
        if action in {"记录回执", "迟到签收", "登记签收"}:
            result = self.record_receipt(**data)
            return (result if result.get("ok") else None), result.get("message", "回执已记录")
        if action in {"核对再次领取集合", "再次领取", "领取补投范围", "核对可投范围"}:
            result = self.claim_redelivery(**data)
            return result, result.get("message", "已核对领取范围")
        if action in {"导出追溯", "回查补投", "查询签收追溯"}:
            return self.trace_batch(**data), "已返回同批次追溯结果"
        return super().run_action(entry_id, action)

    def run_action_payload(self, entry_id: int, payload: Any) -> tuple[dict[str, Any] | None, str]:
        return self.run_action(entry_id, str(getattr(payload, "action", "") or ""), payload)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        pipes = store.rows(self.MODULE)
        base["running_pipes"] = sum(1 for p in pipes if p.get("status") == "运行中")
        base["total_deliveries"] = sum(p.get("_total_deliveries", 0) for p in pipes)
        base["overall_success_rate"] = (
            sum(p.get("_successful_deliveries", 0) for p in pipes) /
            max(sum(p.get("_total_deliveries", 0) for p in pipes), 1)
        )
        base["partial_handoffs"] = len(store.delivery_handoffs)
        base["redelivery_applications"] = len(store.redelivery_requests)
        base["open_redelivery_scopes"] = sum(
            len(h.get("eligible_item_ids", [])) for h in store.delivery_handoffs.values()
        )
        return base


class PartialReceiptService(DeliveryPipeService):
    """语义化别名：部分签收详情与补投资格服务。"""


class RedeliveryService(DeliveryPipeService):
    """语义化别名：补投申请与再次领取范围服务。"""
