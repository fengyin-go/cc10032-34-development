"""批次对账领域服务：聚合批次的统一回执与对账结果查询。

数据流角色: aggregate_sink（生产者）→ batch_reconcile（可供下游查询）→ sink_output（消费者）
增强能力: 批次对账结果查询、对账报告绑定检查点快照、失败批次重试写入
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BatchReconcileService(BaseModuleService):
    MODULE = "batch_reconcile"
    ENTITY = "批次对账结果"
    LIST_FIELDS = ["批次号", "任务名称", "检查点", "成功记录", "失败分区", "可发布", "存量版本"]
    REQUIRED_FIELDS = ["批次号", "任务名称"]
    STATUS_ORDER = ["待发布", "可发布", "写入失败", "已回滚"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = ["重试写入"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "重试写入":
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            batch_id = entry.get("batch_id") or entry.get("批次号", "")
            # 延迟导入避免服务注册顺序问题
            from app.services.aggregate_sink import AggregateSinkService
            result = AggregateSinkService().retry_batch_write(batch_id)
            if not result.get("ok"):
                return None, result.get("error", "批次重试失败")
            receipt = result["receipt"]
            return entry, (
                f"批次 {batch_id} 重试完成：成功 {receipt['persisted']} 条，"
                f"检查点 {'已冻结' if receipt['checkpoint_frozen'] else '未冻结'}，"
                f"可发布={'是' if receipt['publishable'] else '否'}"
            )
        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["publishable"] = sum(1 for r in rows if r.get("publishable"))
        base["versions"] = sorted({str(r.get("存量版本", "1")) for r in rows})
        return base
