"""数据导出领域服务：采集数据批量导出、格式转换与交付路径。

跨模块数据流：
- export_dataset ← quality_check: 质量抽检通过后触发导出
"""
from __future__ import annotations

import json
import random
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExportDatasetService(BaseModuleService):
    MODULE = "export_dataset"
    ENTITY = "导出任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "数据来源", "导出格式", "字段筛选", "行数范围", "压缩方式", "文件路径", "导出时间",
                   "签收结果", "可补投范围", "补投申请", "核对状态"]
    REQUIRED_FIELDS = ["任务编号", "数据来源", "导出格式"]
    STATUS_ORDER = ["待执行", "导出中", "已完成", "已失败"]
    ACTION_RULES = {"发起导出": "导出中", "标记完成": "已完成", "标记失败": "已失败"}

    SUPPORTED_FORMATS = ["CSV", "JSON", "Parquet", "Excel", "XML", "Avro"]
    COMPRESSION = ["none", "gzip", "snappy", "lz4", "zstd"]

    def create_entry(self, values: dict[str, Any]) -> tuple:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        entry["_export_progress"] = 0.0
        entry["_exported_rows"] = 0
        entry["_export_errors"] = []
        entry["_start_ts"] = 0
        entry["_delivery_batch_id"] = values.get("数据批次") or values.get("批次号")
        entry["签收结果"] = entry.get("签收结果") or "尚未回写"
        entry["可补投范围"] = entry.get("可补投范围") or "待投递回执"
        entry["补投申请"] = entry.get("补投申请") or "无"
        entry["核对状态"] = entry.get("核对状态") or "无等待核对"
        store.record_metric("export_dataset", "export_task_created", 1,
                            {"format": values.get("导出格式", ""),
                             "source": values.get("数据来源", "")})
        return entry, []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """导出清单支持按投递批次回查，同时保留通用字段过滤。"""
        safe_filters = dict(filters or {})
        batch_id = safe_filters.pop("batch_id", None) or safe_filters.pop("批次号", None)
        rows, _ = super().list_entries(
            filters=safe_filters, keyword=keyword, status=status, page=1, size=1_000_000)
        if batch_id:
            wanted = str(batch_id)
            rows = [
                row for row in rows
                if str(row.get("_delivery_batch_id") or "") == wanted
                or str(row.get("任务编号") or "") == wanted
                or str(row.get("数据批次") or "") == wanted
                or wanted in str(row.get("数据来源") or "")
            ]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def execute_export(self, entry_id: int, data: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
        """执行数据导出。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        entry["status"] = "导出中"
        entry["_start_ts"] = time.time()
        fmt = entry.get("导出格式", "CSV")
        if fmt not in self.SUPPORTED_FORMATS:
            entry["status"] = "已失败"
            entry["_export_errors"].append(f"不支持的格式：{fmt}")
            entry["abnormal"] = True
            return entry
        # 模拟导出
        self._simulate_export()
        row_count = data if isinstance(data, int) else random.randint(100, 100000)
        entry["_exported_rows"] = row_count if isinstance(row_count, int) else len(data) if data else 0
        entry["行数范围"] = f"1-{entry['_exported_rows']}"
        entry["文件路径"] = f"/exports/{entry.get('任务编号', 'TASK')}.{fmt.lower()}"
        compression = entry.get("压缩方式", "none")
        if compression != "none":
            entry["文件路径"] += f".{compression}"
        entry["导出时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["_export_progress"] = 100.0
        store.record_metric("export_dataset", "export_completed", entry["_exported_rows"],
                            {"format": fmt, "compression": compression})
        return entry

    def export_with_quality_check(self, entry_id: int, quality_check_id: int,
                                  passed: bool) -> dict[str, Any] | None:
        """质量抽检通过后触发导出。跨模块：quality_check → export_dataset"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        if not passed:
            entry["status"] = "已失败"
            entry["abnormal"] = True
            return entry
        store.pipeline_trace(
            "quality_check", "export_dataset", quality_check_id, entry_id,
            "quality_passed", {"action": "trigger_export"}
        )
        return self.execute_export(entry_id)

    def format_to_bytes(self, entry_id: int) -> dict[str, Any] | None:
        """模拟格式转换，返回文件大小。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        sizes = {"CSV": 1.2, "JSON": 1.5, "Parquet": 0.6, "Excel": 2.0, "XML": 2.5, "Avro": 0.4}
        row_factor = entry.get("_exported_rows", 1000) / 1000
        size_mb = sizes.get(entry.get("导出格式", "CSV"), 1.0) * row_factor
        return {"format": entry.get("导出格式"), "rows": entry.get("_exported_rows"),
                "size_mb": round(size_mb, 2), "compression": entry.get("压缩方式", "none")}

    def attach_delivery_batch(self, entry_id: int, batch_id: str,
                              handoff_id: str | int | None = None) -> dict[str, Any] | None:
        """把导出任务/清单行绑定到同一投递批次，并立即回写签收结果。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        from app.services import service_for
        delivery = service_for("delivery_pipe")
        trace = delivery.attach_export(entry_id, batch_id, handoff_id) if delivery else None
        if trace is not None:
            return trace
        entry["_delivery_batch_id"] = batch_id
        entry["核对状态"] = "未找到逐条交接，按历史批次兼容展示"
        return self.delivery_trace(entry_id, batch_id)

    def delivery_trace(self, entry_id: int | None = None,
                       batch_id: str | None = None) -> dict[str, Any] | None:
        """从导出端按同一批次回查原回执、补投申请和等待核对项。"""
        entry = store.find(self.MODULE, entry_id) if entry_id is not None else None
        if entry_id is not None and entry is None:
            return None
        if batch_id is None and entry is not None:
            batch_id = entry.get("_delivery_batch_id") or entry.get("任务编号")

        from app.services import service_for
        delivery = service_for("delivery_pipe")
        if delivery is None:
            return {"ok": False, "message": "投递服务不可用"}
        trace = delivery.trace_batch(batch_id=batch_id, export_id=entry_id)
        if entry is not None and batch_id:
            entry["_delivery_batch_id"] = batch_id
            entry["_delivery_trace"] = trace
        return trace

    query_delivery_trace = delivery_trace
    get_delivery_trace = delivery_trace
    get_receipt_results = delivery_trace
    query_receipts = delivery_trace

    def delivery_manifest_rows(self, entry_id: int | None = None,
                               batch_id: str | None = None) -> list[dict[str, Any]]:
        """返回可写入 CSV/清单的条目级签收与补投追踪行。"""
        trace = self.delivery_trace(entry_id, batch_id) or {}
        rows: list[dict[str, Any]] = []
        for item in trace.get("items", []):
            redelivery_ids = item.get("redelivery_ids", [])
            rows.append({
                "batch_id": trace.get("batch_id", batch_id or ""),
                "item_id": item.get("item_id", ""),
                "签收结果": item.get("签收状态") or item.get("status", ""),
                "status": item.get("status", ""),
                "signed": bool(item.get("signed")),
                "failed": bool(item.get("failed")),
                "waiting_verification": bool(item.get("waiting_verification")),
                "receipt_ids": ",".join(item.get("receipt_ids", []) or []),
                "redelivery_ids": ",".join(redelivery_ids or []),
                "claim_ids": ",".join(item.get("claim_ids", []) or []),
                "可补投": not bool(item.get("signed")),
            })
        return rows

    get_delivery_manifest = delivery_manifest_rows
    get_manifest_rows = delivery_manifest_rows

    def _simulate_export(self) -> None:
        time.sleep(random.uniform(0.05, 0.2))

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        tasks = store.rows(self.MODULE)
        base["completed_exports"] = sum(1 for t in tasks if t.get("status") == "已完成")
        base["total_rows_exported"] = sum(t.get("_exported_rows", 0) for t in tasks)
        return base