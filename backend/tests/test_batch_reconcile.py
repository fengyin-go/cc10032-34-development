"""批次对账与序号34迁移切换测试。

测试用例：
  - test_flush_creates_unified_receipt：统一回执（落库+检查点+完整性核对）同时完成
  - test_flush_idempotent_same_batch：每个聚合批次只能有一个可消费对账结果
  - test_flush_failure_not_publishable：写入失败时不得把批次标记为可发布
  - test_retry_completes_batch_and_freezes_checkpoint：整批写入完成后冻结检查点
  - test_sink_reconcile_batch_recalculates：结果落地模块重新计算成功/失败/分区统计
  - test_completeness_report：输出完整性报告
  - test_migration_switch_idempotent：序号34并发/重复切换只生效一次
  - test_migration_switch_rollback_on_desync：事务失步后回滚原版本
  - test_reconcile_action_retry：批次对账页面重试写入动作
  - test_flush_action_from_task_page：聚合任务页面发起写入动作
"""
from __future__ import annotations

import time

import pytest

from app.services.aggregate_sink import (
    AggregateSinkService,
    MIGRATION_LEDGER,
    RECONCILE_MODULE,
)
from app.services.batch_reconcile import BatchReconcileService
from app.services.sink_output import SinkOutputService
from app.store import store

_agg = AggregateSinkService()
_sink = SinkOutputService()
_reconcile = BatchReconcileService()


def _unique(prefix: str) -> str:
    return f"{prefix}_{int(time.time() * 1_000_000)}"


def _make_task(name: str) -> dict:
    entry, missing = _agg.create_entry({
        "任务名称": name,
        "源指标集": "metric_x",
        "聚合粒度": "hour",
        "输出表名": "dm_test",
        "落库频率": "60",
        "覆盖模式": "append",
        "分区键": "dt",
    })
    assert not missing
    return entry


def _records(count: int, fail_indexes: tuple[int, ...] = ()) -> list[dict]:
    return [
        {"metric": f"m_{i}", "value": i * 10, "_force_fail": i in fail_indexes}
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# 统一回执：读取聚合结果 → 生成聚合落库数据 → 核对输出完整性报告
# ---------------------------------------------------------------------------

class TestFlushWithReceipt:
    """统一回执：聚合落库数据与检查点必须同时完成。"""

    def test_flush_creates_unified_receipt(self):
        task_name = _unique("任务A")
        _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(5))
        assert result["ok"] is True
        receipt = result["receipt"]
        # 读取聚合结果
        assert receipt["read_count"] == 5
        # 生成聚合落库数据
        assert receipt["persisted"] == 5
        assert receipt["failed"] == 0
        # 检查点同时完成并冻结
        assert receipt["checkpoint_id"] == f"cp_{result['batch_id']}"
        assert receipt["checkpoint_frozen"] is True
        # 核对输出完整性报告
        assert receipt["completeness"]["complete"] is True
        assert receipt["publishable"] is True

        # 批次对账结果可供下游查询
        reconcile = _agg.get_batch_reconcile(result["batch_id"])
        assert reconcile is not None
        assert reconcile["status"] == "可发布"
        assert reconcile["可发布"] == "是"
        assert reconcile["成功记录"] == 5

        # 检查点快照已落地
        cp = next(
            c for c in store.rows("checkpoint_mgr")
            if c.get("checkpoint_id") == receipt["checkpoint_id"]
        )
        assert cp["operator_states"]["batch_id"] == result["batch_id"]
        assert cp["frozen"] is True

    def test_flush_idempotent_same_batch(self):
        """每个聚合批次只能有一个可消费对账结果，重复写入只生效一次。"""
        task_name = _unique("任务B")
        _make_task(task_name)
        batch_id = _unique("batch")
        first = _agg.flush_batch_with_receipt(task_name, _records(4), batch_id=batch_id)
        sink_count = sum(1 for r in store.rows("sink_output") if r.get("batch_id") == batch_id)
        cp_count = sum(
            1 for c in store.rows("checkpoint_mgr")
            if c.get("checkpoint_id") == f"cp_{batch_id}"
        )
        second = _agg.flush_batch_with_receipt(task_name, _records(4), batch_id=batch_id)
        assert second["idempotent"] is True
        assert second["receipt"]["batch_id"] == batch_id
        # 落库数据、检查点、对账结果均未重复生成
        assert sum(
            1 for r in store.rows("sink_output") if r.get("batch_id") == batch_id
        ) == sink_count
        assert sum(
            1 for c in store.rows("checkpoint_mgr")
            if c.get("checkpoint_id") == f"cp_{batch_id}"
        ) == cp_count
        assert sum(
            1 for b in store.rows(RECONCILE_MODULE) if b.get("batch_id") == batch_id
        ) == 1
        assert first["receipt"]["persisted"] == second["receipt"]["persisted"]

    def test_flush_failure_not_publishable(self):
        """写入失败和检查点成功可同时出现，但批次不得标记为可发布。"""
        task_name = _unique("任务C")
        task = _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(5, fail_indexes=(1, 3)))
        assert result["ok"] is True
        receipt = result["receipt"]
        assert receipt["persisted"] == 3
        assert receipt["failed"] == 2
        # 检查点已创建但未冻结
        assert receipt["checkpoint_id"]
        assert receipt["checkpoint_frozen"] is False
        # 批次不可发布
        assert receipt["publishable"] is False
        reconcile = _agg.get_batch_reconcile(result["batch_id"])
        assert reconcile["status"] == "写入失败"
        assert reconcile["可发布"] == "否"
        assert reconcile["失败分区"] != "无"
        # 聚合任务状态进入写入失败
        assert task["status"] == "写入失败"

    def test_retry_completes_batch_and_freezes_checkpoint(self):
        """写入重试与检查点创建并发：等整批写入完成再冻结检查点。"""
        task_name = _unique("任务D")
        task = _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(4, fail_indexes=(2,)))
        batch_id = result["batch_id"]
        assert result["receipt"]["publishable"] is False

        retry = _agg.retry_batch_write(batch_id)
        assert retry["ok"] is True
        assert retry["retried"] == 1
        receipt = retry["receipt"]
        # 页面状态最终停靠到一个完整批次状态
        assert receipt["publishable"] is True
        assert receipt["persisted"] == 4
        assert receipt["failed"] == 0
        assert receipt["checkpoint_frozen"] is True
        assert receipt["completeness"]["complete"] is True
        reconcile = _agg.get_batch_reconcile(batch_id)
        assert reconcile["status"] == "可发布"
        assert reconcile["成功记录"] == 4
        assert task["status"] == "运行中"

        # 重复重试幂等
        again = _agg.retry_batch_write(batch_id)
        assert again["idempotent"] is True

    def test_retry_unknown_batch(self):
        result = _agg.retry_batch_write("batch_不存在")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# 结果落地模块：重新计算成功、失败和分区统计，对账报告绑定检查点快照
# ---------------------------------------------------------------------------

class TestSinkReconcile:
    def test_sink_reconcile_batch_recalculates(self):
        task_name = _unique("任务E")
        _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(6, fail_indexes=(4,)))
        batch_id = result["batch_id"]

        report = _sink.reconcile_batch(batch_id)
        assert report["ok"] is True
        assert report["total"] == 6
        assert report["success"] == 5
        assert report["failed"] == 1
        # 分区统计
        assert len(report["partitions"]) >= 1
        assert report["failed_partitions"], "应存在失败分区"
        # 对账报告绑定检查点快照
        assert report["checkpoint_id"] == f"cp_{batch_id}"
        assert report["checkpoint_snapshot_ts"] > 0
        assert report["checkpoint_frozen"] is False
        assert report["publishable"] is False

        # 重试后重新计算，批次完整可发布
        _agg.retry_batch_write(batch_id)
        report2 = _sink.reconcile_batch(batch_id)
        assert report2["failed"] == 0
        assert report2["checkpoint_frozen"] is True
        assert report2["publishable"] is True

    def test_completeness_report(self):
        task_name = _unique("任务F")
        _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(3))
        batch_id = result["batch_id"]
        report = _sink.batch_completeness_report(batch_id)
        assert report["ok"] is True
        assert report["expected"] == 3
        assert report["actual"] == 3
        assert report["success"] == 3
        assert report["complete"] is True
        assert report["publishable"] is True

    def test_completeness_unknown_batch(self):
        report = _sink.batch_completeness_report("batch_不存在")
        assert report["ok"] is False


# ---------------------------------------------------------------------------
# 序号34：迁移切换幂等 + 事务失步回滚原版本
# ---------------------------------------------------------------------------

class TestMigrationSwitch:
    def test_migration_switch_idempotent(self):
        """序号34的并发/重复切换只生效一次，并保持幂等。"""
        task_name = _unique("任务G")
        _make_task(task_name)
        flushed = _agg.flush_batch_with_receipt(task_name, _records(3))
        v0 = _agg.get_batch_reconcile(flushed["batch_id"])["version"]
        switch_id = _unique("34")

        first = _agg.apply_migration_switch(switch_id=switch_id, task_name=task_name)
        assert first["applied"] is True
        assert first["entry"]["status"] == "已生效"
        assert first["entry"]["from_version"] == v0
        assert first["entry"]["to_version"] == v0 + 1

        # 迁移切换产生不同存量版本
        batches = [
            b for b in store.rows(RECONCILE_MODULE)
            if b.get("任务名称") == task_name
        ]
        assert all(b["version"] == v0 + 1 for b in batches)
        assert all(b["存量版本"] == str(v0 + 1) for b in batches)

        # 重复切换只生效一次
        second = _agg.apply_migration_switch(switch_id=switch_id, task_name=task_name)
        assert second["idempotent"] is True
        assert second["applied"] is False
        assert second["entry"]["to_version"] == v0 + 1
        assert all(b["version"] == v0 + 1 for b in batches)
        # 台账中只有一条该序号的记录
        entries = [
            e for e in store.rows(MIGRATION_LEDGER)
            if e.get("switch_id") == switch_id
        ]
        assert len(entries) == 1

    def test_migration_switch_rollback_on_desync(self):
        """序号34事务失步后回滚原版本。"""
        task_name = _unique("任务H")
        _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(3))
        batch_id = result["batch_id"]

        # 制造事务失步：额外写入一条成功落库记录，使对账结果与落库数据不一致
        store.rows("sink_output").append({
            "id": 999999,
            "batch_id": batch_id,
            "write_status": "success",
            "partition": "tamper",
        })
        v0 = _agg.get_batch_reconcile(batch_id)["version"]
        switch_id = _unique("34rb")
        switched = _agg.apply_migration_switch(switch_id=switch_id, task_name=task_name)
        assert switched["rolled_back"] is True
        assert switched["ok"] is False
        entry = switched["entry"]
        assert entry["status"] == "已回滚"
        assert batch_id in entry["desync_batches"]
        # 回滚原版本：版本号保持切换前的值
        assert entry["to_version"] == entry["from_version"]
        reconcile = _agg.get_batch_reconcile(batch_id)
        assert reconcile["version"] == v0
        assert reconcile["存量版本"] == str(v0)
        assert reconcile["status"] == "已回滚"
        assert reconcile["可发布"] == "否"

        # 回滚后重复切换仍只返回首次结果
        again = _agg.apply_migration_switch(switch_id=switch_id, task_name=task_name)
        assert again["idempotent"] is True
        assert again["entry"]["status"] == "已回滚"

    def test_migration_ledger_query(self):
        """迁移台账可供其他页面同步查询。"""
        task_name = _unique("任务I")
        _make_task(task_name)
        _agg.flush_batch_with_receipt(task_name, _records(2))
        switch_id = _unique("34ledger")
        _agg.apply_migration_switch(switch_id=switch_id, task_name=task_name)
        ledger = _agg.migration_ledger()
        mine = [e for e in ledger if e.get("switch_id") == switch_id]
        assert len(mine) == 1
        assert mine[0]["to_version"] == mine[0]["from_version"] + 1


# ---------------------------------------------------------------------------
# 页面动作：聚合任务页面发起写入、批次对账页面重试
# ---------------------------------------------------------------------------

class TestPageActions:
    def test_flush_action_from_task_page(self):
        """复核人员在聚合任务页面发起写入。"""
        task_name = _unique("任务J")
        task = _make_task(task_name)
        entry, message = _agg.run_action(int(task["id"]), "发起写入")
        assert entry is not None
        assert "写入完成" in message
        assert "可发布=是" in message
        # 批次对账结果已生成，可在结果落地页面核对
        batches = [
            b for b in store.rows(RECONCILE_MODULE)
            if b.get("任务名称") == task_name
        ]
        assert len(batches) == 1
        report = _sink.reconcile_batch(batches[0]["batch_id"])
        assert report["success"] == 5
        assert report["publishable"] is True

    def test_reconcile_action_retry(self):
        """批次对账页面对失败批次执行重试写入。"""
        task_name = _unique("任务K")
        _make_task(task_name)
        result = _agg.flush_batch_with_receipt(task_name, _records(4, fail_indexes=(0,)))
        batch_id = result["batch_id"]
        reconcile = _agg.get_batch_reconcile(batch_id)

        entry, message = _reconcile.run_action(int(reconcile["id"]), "重试写入")
        assert entry is not None
        assert "重试完成" in message
        assert "可发布=是" in message
        assert _agg.get_batch_reconcile(batch_id)["status"] == "可发布"

    def test_downstream_query_batches(self):
        """批次对账结果可供下游查询。"""
        task_name = _unique("任务L")
        _make_task(task_name)
        _agg.flush_batch_with_receipt(task_name, _records(2))
        items = _agg.list_batch_reconciles()
        assert any(b.get("任务名称") == task_name for b in items)
        # 通用列表接口同样可查
        rows, total = _reconcile.list_entries(filters={"任务名称": task_name})
        assert total == 1
        assert rows[0]["可发布"] == "是"
