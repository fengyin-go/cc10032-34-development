"""聚合批次对账接口：统一回执、批次查询、输出完整性报告与序号34迁移切换。

注意：本路由必须先于通用资源路由注册，避免 /api/agg/* 被 /api/{module} 抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import FlushBatchPayload, MigrationSwitchPayload, RetryBatchPayload
from app.services.aggregate_sink import AggregateSinkService
from app.services.sink_output import SinkOutputService

router = APIRouter(prefix="/api/agg", tags=["聚合批次对账"])

aggregate_sink = AggregateSinkService()
sink_output = SinkOutputService()


@router.post("/flush")
def flush_batch(payload: FlushBatchPayload) -> dict[str, Any]:
    """发起批次写入：统一回执（读取聚合结果→落库→检查点→完整性核对），幂等。"""
    records = payload.records
    if records is None:
        records = aggregate_sink.simulate_batch_records(payload.task_name)
        if records is None:
            raise HTTPException(status_code=404, detail=f"聚合任务「{payload.task_name}」不存在")
    result = aggregate_sink.flush_batch_with_receipt(
        payload.task_name, records, batch_id=payload.batch_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "批次写入失败"))
    return result


@router.post("/retry")
def retry_batch(payload: RetryBatchPayload) -> dict[str, Any]:
    """重试失败写入：等整批写入完成再冻结检查点，批次最终停靠到完整状态。"""
    result = aggregate_sink.retry_batch_write(payload.batch_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "批次不存在"))
    return result


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    """下游查询批次对账结果。"""
    items = aggregate_sink.list_batch_reconciles()
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}")
def get_batch_report(batch_id: str) -> dict[str, Any]:
    """批次对账报告：结果落地模块重新计算成功、失败和分区统计，绑定检查点快照。"""
    report = sink_output.reconcile_batch(batch_id)
    if not report.get("ok"):
        raise HTTPException(status_code=404, detail=report.get("error", "批次不存在"))
    return report


@router.get("/batches/{batch_id}/completeness")
def get_batch_completeness(batch_id: str) -> dict[str, Any]:
    """输出完整性报告：按批次核对聚合落库数据是否完整可发布。"""
    report = sink_output.batch_completeness_report(batch_id)
    if not report.get("ok"):
        raise HTTPException(status_code=404, detail=report.get("error", "批次不存在"))
    return report


@router.post("/migration_switch")
def migration_switch(payload: MigrationSwitchPayload) -> dict[str, Any]:
    """序号34迁移切换：并发/重复切换只生效一次，事务失步回滚原版本。"""
    return aggregate_sink.apply_migration_switch(
        switch_id=payload.switch_id, task_name=payload.task_name)


@router.get("/migration_ledger")
def get_migration_ledger() -> dict[str, Any]:
    """迁移台账：供部署页面与其他页面同步查询迁移前后版本。"""
    items = aggregate_sink.migration_ledger()
    return {"items": items, "total": len(items)}
