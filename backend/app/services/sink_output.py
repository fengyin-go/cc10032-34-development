"""结果落地领域服务：管理流式计算结果的输出落地配置。

数据流角色: state_store / calc_operator → sink_output（消费者+生产者）
增强能力: 批量写入调度、分区策略计算、写入模式切换、异常重试、输出完整性校验
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SinkOutputService(BaseModuleService):
    MODULE = "sink_output"
    ENTITY = "输出配置"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "写入模式", "分区策略", "格式转换", "批量大小"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "写入异常"]
    ACTION_RULES = {"启用输出": "草稿", "停用输出": "已生效"}
    NEGATIVE_ACTIONS = ["停用输出"]

    # ---- 跨模块数据消费与生产 ----

    def batch_write_results(
        self, sink_name: str, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """将计算结果批量写入目标存储，支持分区和格式转换。"""
        sink = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == sink_name:
                sink = r
                break
        if sink is None:
            return {"ok": False, "error": f"输出配置「{sink_name}」不存在"}
        if sink.get("status") == "已停用":
            return {"ok": False, "error": f"输出配置「{sink_name}」已停用"}

        try:
            batch_size = int(sink.get("批量大小", "100"))
        except (ValueError, TypeError):
            batch_size = 100

        partition_strategy = sink.get("分区策略", "round_robin")
        write_mode = sink.get("写入模式", "append")
        output_type = sink.get("输出类型", "database")
        format_type = sink.get("格式转换", "json")

        written = 0
        failed = 0
        for i, item in enumerate(results):
            # 分区键计算
            partition_key = self._calc_partition(
                item, partition_strategy, i,
            )
            # 格式转换
            converted = self._convert_format(item, format_type)
            record = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
                ) + 1,
                "sink": sink_name,
                "output_type": output_type,
                "partition": partition_key,
                "format": format_type,
                "mode": write_mode,
                "data": converted,
                "written_at": time.time(),
                "status": "已生效",
            }
            # 模拟写入成功/失败
            if self._simulate_write(partition_key):
                record["write_status"] = "success"
                store.rows(self.MODULE).append(record)
                written += 1
            else:
                record["write_status"] = "failed"
                record["异常标记"] = "写入异常"
                store.rows(self.MODULE).append(record)
                failed += 1

        if failed > 0:
            sink["status"] = "写入异常"
            sink["pending"] = True

        return {
            "ok": True,
            "sink": sink_name,
            "total": len(results),
            "written": written,
            "failed": failed,
            "partition_strategy": partition_strategy,
        }

    @staticmethod
    def _calc_partition(
        item: dict[str, Any], strategy: str, index: int,
    ) -> str:
        """计算数据分区键。"""
        if strategy == "hash_key":
            raw = str(item.get("key", index))
            return f"pt_{hashlib.md5(raw.encode()).hexdigest()[:4]}"
        elif strategy == "time_range":
            ts = item.get("timestamp", time.time())
            return time.strftime("dt=%Y%m%d%H", time.localtime(ts))
        elif strategy == "source_based":
            return f"src_{item.get('source', 'default')}"
        else:  # round_robin
            return f"pt_{index % 8:02d}"

    @staticmethod
    def _convert_format(
        item: dict[str, Any], fmt: str,
    ) -> dict[str, Any]:
        """格式转换：JSON/CSV/Parquet 模拟。"""
        if fmt == "csv":
            return {"format": "csv", "raw": str(item)}
        elif fmt == "parquet":
            return {"format": "parquet", "columns": list(item.keys()), "rows": 1}
        return {"format": "json", "payload": item}

    @staticmethod
    def _simulate_write(partition: str) -> bool:
        """模拟写入成功（绝大部分成功，偶发失败模拟异常）。"""
        return hash(partition) % 20 != 0

    def verify_output_integrity(self, sink_name: str) -> dict[str, Any]:
        """验证指定 sink 下的输出记录完整性和写入成功率。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name
        ]
        total = len(records)
        success = sum(1 for r in records if r.get("write_status") == "success")
        failed = total - success
        partitions = set(r.get("partition", "") for r in records)
        return {
            "ok": True,
            "sink": sink_name,
            "total": total,
            "success": success,
            "failed": failed,
            "success_rate": success / max(total, 1),
            "partitions": len(partitions),
            "needs_retry": failed > 0,
        }

    def retry_failed_writes(self, sink_name: str) -> dict[str, Any]:
        """重试失败的写入记录。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name and r.get("write_status") == "failed"
        ]
        retried = 0
        for record in records:
            if self._simulate_write(record.get("partition", "")):
                record["write_status"] = "success"
                retried += 1
        return {
            "ok": True,
            "sink": sink_name,
            "total_failed": len(records),
            "retried_success": retried,
        }

    # ---- 批次对账：读取聚合落库数据并重新计算 ----

    def reconcile_batch(self, batch_id: str) -> dict[str, Any]:
        """批次对账报告：读取聚合落库数据，重新计算成功、失败和分区统计。

        对账报告绑定检查点快照，供结果落地页面判断批次是否完整可发布。
        """
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("batch_id") == batch_id
        ]
        reconcile = next(
            (b for b in store.rows("batch_reconcile")
             if b.get("batch_id") == batch_id or b.get("批次号") == batch_id),
            None,
        )
        if not records and reconcile is None:
            return {"ok": False, "error": f"批次「{batch_id}」没有聚合落库数据"}

        success = sum(1 for r in records if r.get("write_status") == "success")
        failed = len(records) - success
        partitions: dict[str, dict[str, int]] = {}
        for r in records:
            part = str(r.get("partition", ""))
            stat = partitions.setdefault(part, {"success": 0, "failed": 0})
            stat["success" if r.get("write_status") == "success" else "failed"] += 1
        failed_partitions = sorted(p for p, s in partitions.items() if s["failed"] > 0)

        # 绑定检查点快照
        checkpoint = None
        for row in store.rows("checkpoint_mgr"):
            states = row.get("operator_states") or {}
            if row.get("checkpoint_id") == f"cp_{batch_id}" or states.get("batch_id") == batch_id:
                checkpoint = row
                break

        publishable = (
            failed == 0
            and checkpoint is not None
            and bool(checkpoint.get("frozen"))
        )
        return {
            "ok": True,
            "batch_id": batch_id,
            "total": len(records),
            "success": success,
            "failed": failed,
            "partitions": partitions,
            "failed_partitions": failed_partitions,
            "checkpoint_id": (checkpoint or {}).get("checkpoint_id"),
            "checkpoint_snapshot_ts": (checkpoint or {}).get("snapshot_ts"),
            "checkpoint_frozen": bool((checkpoint or {}).get("frozen")),
            "publishable": publishable,
            "status": (reconcile or {}).get("status"),
            "version": (reconcile or {}).get("version", 1),
        }

    def batch_completeness_report(self, batch_id: str) -> dict[str, Any]:
        """输出完整性报告：按批次核对聚合落库数据是否完整可发布。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("batch_id") == batch_id
        ]
        reconcile = next(
            (b for b in store.rows("batch_reconcile")
             if b.get("batch_id") == batch_id or b.get("批次号") == batch_id),
            None,
        )
        if not records and reconcile is None:
            return {"ok": False, "error": f"批次「{batch_id}」不存在"}

        expected = int((reconcile or {}).get("read_count", len(records)))
        success = sum(1 for r in records if r.get("write_status") == "success")
        failed = len(records) - success
        return {
            "ok": True,
            "batch_id": batch_id,
            "expected": expected,
            "actual": len(records),
            "success": success,
            "failed": failed,
            "complete": len(records) >= expected and failed == 0,
            "publishable": bool((reconcile or {}).get("publishable")),
        }

    def list_sink_destinations(self) -> list[dict[str, Any]]:
        """列出所有活跃的输出目标和状态。"""
        configs = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for c in configs:
            name = c.get("配置名称", "")
            records_count = sum(
                1 for r in store.rows(self.MODULE)
                if r.get("sink") == name
            )
            result.append({
                "sink": name,
                "type": c.get("输出类型", ""),
                "target": c.get("目标地址", ""),
                "status": c.get("status", ""),
                "records": records_count,
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_records"] = sum(
            1 for r in rows if r.get("write_status")
        )
        base["failed_records"] = sum(
            1 for r in rows if r.get("write_status") == "failed"
        )
        return base