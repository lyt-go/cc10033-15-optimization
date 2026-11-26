"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
新增跨模块数据通道：pipeline_log 记录跨模块流转日志，service_registry 维护服务引用。
"""
from __future__ import annotations

import threading
import time
from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._pipeline_log: list[dict[str, Any]] = []
        self._service_registry: dict[str, Any] = {}
        self._metrics: dict[str, list[dict[str, Any]]] = {}
        self._event_queue: list[dict[str, Any]] = []

        # 投递回执/补投申请的内存台账。键均为业务编号，服务层负责一致性。
        self.delivery_handoffs: dict[str, dict[str, Any]] = {}
        self.delivery_batch_index: dict[str, list[str]] = {}
        self.redelivery_requests: dict[str, dict[str, Any]] = {}
        self.redelivery_by_batch: dict[str, list[str]] = {}
        self.delivery_claims: dict[str, dict[str, Any]] = {}
        self.delivery_receipt_ids: dict[str, dict[str, Any]] = {}
        self.delivery_idempotency: dict[str, dict[str, Any]] = {}
        self.delivery_lock = threading.RLock()

    # ── 基础存取 ──────────────────────────────

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if str(row.get(field, "")) == str(value)]

    def upsert(self, module: str, entry: dict[str, Any]) -> dict[str, Any]:
        rows = self.rows(module)
        eid = entry.get("id")
        if eid is not None:
            for i, row in enumerate(rows):
                if row.get("id") == eid:
                    rows[i].update(entry)
                    return rows[i]
        max_id = max((int(r.get("id", 0)) for r in rows), default=0)
        entry["id"] = max_id + 1
        rows.append(entry)
        return entry

    # ── 跨模块管道日志 ─────────────────────────

    def pipeline_trace(
        self,
        from_module: str,
        to_module: str,
        from_id: int,
        to_id: int,
        action: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """记录跨模块数据流日志，用于审计和题位源码锚点。"""
        self._pipeline_log.append({
            "ts": time.time(),
            "iso": "",  # 预留，调用方按需覆盖
            "from_module": from_module,
            "to_module": to_module,
            "from_id": from_id,
            "to_id": to_id,
            "action": action,
            "detail": detail or {},
        })

    def pipeline_log(self) -> list[dict[str, Any]]:
        return list(self._pipeline_log)

    # ── 指标收集 ──────────────────────────────

    def record_metric(self, module: str, metric_name: str, value: float, tags: dict[str, str] | None = None) -> None:
        self._metrics.setdefault(module, []).append({
            "ts": time.time(),
            "name": metric_name,
            "value": value,
            "tags": tags or {},
        })

    def metrics(self, module: str | None = None) -> list[dict[str, Any]]:
        if module:
            return list(self._metrics.get(module, []))
        result = []
        for mod_metrics in self._metrics.values():
            result.extend(mod_metrics)
        return result

    # ── 事件队列（异步编排预留） ─────────────────

    def enqueue_event(self, event_type: str, payload: dict[str, Any]) -> None:
        self._event_queue.append({"type": event_type, "payload": payload, "ts": time.time()})

    def dequeue_event(self) -> dict[str, Any] | None:
        return self._event_queue.pop(0) if self._event_queue else None

    def pending_events(self) -> int:
        return len(self._event_queue)

    # ── 拓扑概览 ──────────────────────────────

    def get_pipeline_topology(self) -> dict[str, list[str]]:
        """返回跨模块数据流拓扑，用于可视化题位。"""
        edges = {}
        for log in self._pipeline_log:
            fm = log["from_module"]
            tm = log["to_module"]
            edges.setdefault(fm, [])
            if tm not in edges[fm]:
                edges[fm].append(tm)
        return edges

    # ── 看板概览 ──────────────────────────────

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()