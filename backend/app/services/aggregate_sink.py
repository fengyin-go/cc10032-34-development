"""聚合落库领域服务：将实时聚合结果按定时频率批量写入目标存储表。

增强能力: 聚合粒度校验、落库频率管理、分区键生成、覆盖/追加模式、与检查点对齐
批次对账: 统一回执（读取聚合结果 → 生成聚合落库数据 → 核对输出完整性报告）、
         每个聚合批次只产生一个可消费对账结果（幂等）、聚合落库数据与检查点同时完成、
         写入失败不得标记可发布、重试完成后冻结检查点、序号34迁移切换（只生效一次、
         事务失步回滚原版本、迁移台账可供其他页面查询）
"""
from __future__ import annotations

import threading
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.sink_output import SinkOutputService
from app.store import store

# 批次对账结果表与迁移台账表
RECONCILE_MODULE = "batch_reconcile"
MIGRATION_LEDGER = "migration_ledger"
# 序号34：默认迁移切换序号
DEFAULT_SWITCH_ID = "34"

# 并发写入/并发切换只生效一次：批次与切换级别的互斥锁
_BATCH_LOCK = threading.Lock()
_SWITCH_LOCK = threading.Lock()


class AggregateSinkService(BaseModuleService):
    MODULE = "aggregate_sink"
    ENTITY = "聚合任务"
    LIST_FIELDS = ["任务名称", "源指标集", "聚合粒度", "输出表名", "落库频率", "覆盖模式", "分区键"]
    REQUIRED_FIELDS = ["任务名称", "源指标集", "聚合粒度"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "写入失败"]
    ACTION_RULES = {"启动任务": "待启动", "停止任务": "运行中"}
    NEGATIVE_ACTIONS = []

    # ---- 基础查找 ----

    def _find_task(self, task_name: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if r.get("任务名称") == task_name:
                return r
        return None

    @staticmethod
    def _find_checkpoint(checkpoint_id: str) -> dict[str, Any] | None:
        if not checkpoint_id:
            return None
        for r in store.rows("checkpoint_mgr"):
            if r.get("checkpoint_id") == checkpoint_id:
                return r
        return None

    @staticmethod
    def _next_id(module: str) -> int:
        ids = []
        for r in store.rows(module):
            try:
                ids.append(int(r.get("id", 0)))
            except (ValueError, TypeError):
                continue
        return max(ids, default=0) + 1

    # ---- 批次对账：统一回执 ----

    def get_batch_reconcile(self, batch_id: str) -> dict[str, Any] | None:
        """按批次号查询批次对账结果（下游查询入口）。"""
        for b in store.rows(RECONCILE_MODULE):
            if b.get("batch_id") == batch_id or b.get("批次号") == batch_id:
                return b
        return None

    def list_batch_reconciles(self) -> list[dict[str, Any]]:
        """列出全部批次对账结果，供下游与迁移后页面核对。"""
        return [dict(b) for b in store.rows(RECONCILE_MODULE)]

    def simulate_batch_records(
        self, task_name: str, count: int = 5,
    ) -> list[dict[str, Any]] | None:
        """读取聚合结果（模拟）：按任务配置生成一批待落库的聚合记录。"""
        task = self._find_task(task_name)
        if task is None:
            return None
        base_metric = task.get("源指标集", "metric")
        grain = task.get("聚合粒度", "hour")
        now = time.time()
        return [
            {
                "metric": f"{base_metric}_{i}",
                "granularity": grain,
                "value": (i + 1) * 10,
                "ts": now,
            }
            for i in range(count)
        ]

    def flush_batch_with_receipt(
        self,
        task_name: str,
        aggregated: list[dict[str, Any]],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """统一回执：读取聚合结果 → 生成聚合落库数据 → 同步检查点 → 核对输出完整性。

        约束：
          - 每个聚合批次只能有一个可消费对账结果（按 batch_id 幂等，重复调用返回原回执）；
          - 聚合落库数据与检查点必须同时完成；
          - 写入失败时批次不得标记为可发布，检查点先建不冻结，等整批写入完成再冻结。
        """
        task = self._find_task(task_name)
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}
        batch_id = batch_id or f"batch_{task_name}_{int(time.time() * 1000)}"

        with _BATCH_LOCK:
            existing = self.get_batch_reconcile(batch_id)
            if existing is not None:
                # 幂等：同批次重复写入只生效一次，返回已生成的统一回执
                return {
                    "ok": True,
                    "batch_id": batch_id,
                    "idempotent": True,
                    "receipt": self._build_receipt(existing),
                }

            partition_key = task.get("分区键") or "dt"
            mode = task.get("覆盖模式") or "append"
            partition_val = time.strftime("%Y%m%d%H")

            # 1) 读取聚合结果 → 生成聚合落库数据（写入结果落地表，带批次号）
            success_records: list[dict[str, Any]] = []
            failed_records: list[dict[str, Any]] = []
            for item in aggregated:
                record = {
                    "id": self._next_id("sink_output"),
                    "配置名称": f"agg_{task_name}_{partition_val}",
                    "输出类型": "aggregate_batch",
                    "目标地址": f"table://{task.get('输出表名', task_name)}",
                    "写入模式": mode,
                    "分区策略": partition_key,
                    "格式转换": "json",
                    "status": "已生效",
                    "sink": f"agg_{task_name}",
                    "batch_id": batch_id,
                    "partition": partition_val,
                    "data": item,
                    "written_at": time.time(),
                }
                if item.get("_force_fail"):
                    record["write_status"] = "failed"
                    record["异常标记"] = "写入异常"
                    failed_records.append(record)
                else:
                    record["write_status"] = "success"
                    success_records.append(record)
                store.rows("sink_output").append(record)

            # 2) 同步创建检查点：聚合落库数据与检查点必须同时完成。
            #    存在失败记录时检查点先建不冻结，等待整批写入完成再冻结。
            frozen = not failed_records
            cp_id = f"cp_{batch_id}"
            cp_entry = {
                "id": self._next_id("checkpoint_mgr"),
                "配置名称": f"checkpoint_{task_name}_{batch_id}",
                "间隔时间": task.get("落库频率", "60"),
                "超时时长": "120",
                "最大并发": "3",
                "对齐策略": "exactly_once",
                "保留数量": 5,
                "外部化路径": f"/data/checkpoints/{task_name}",
                "status": "已启用",
                "checkpoint_id": cp_id,
                "operator_states": {
                    "task": task_name,
                    "batch_id": batch_id,
                    "written": len(success_records),
                },
                "snapshot_ts": time.time(),
                "frozen": frozen,
            }
            store.rows("checkpoint_mgr").append(cp_entry)

            # 3) 生成批次对账结果；写入失败时不得把批次标记为可发布
            publishable = not failed_records
            version = self._current_storage_version()
            reconcile = {
                "id": self._next_id(RECONCILE_MODULE),
                "批次号": batch_id,
                "任务名称": task_name,
                "检查点": cp_id,
                "成功记录": len(success_records),
                "失败分区": partition_val if failed_records else "无",
                "可发布": "是" if publishable else "否",
                "存量版本": str(version),
                "status": "可发布" if publishable else "写入失败",
                "pending": not publishable,
                "abnormal": not publishable,
                "batch_id": batch_id,
                "checkpoint_id": cp_id,
                "publishable": publishable,
                "version": version,
                "read_count": len(aggregated),
                "created_at": time.time(),
            }
            store.rows(RECONCILE_MODULE).append(reconcile)

            task["status"] = "运行中" if publishable else "写入失败"
            task["pending"] = not publishable

            return {
                "ok": True,
                "batch_id": batch_id,
                "idempotent": False,
                "receipt": self._build_receipt(reconcile),
            }

    def retry_batch_write(self, batch_id: str) -> dict[str, Any]:
        """重试失败写入：等整批写入完成再冻结检查点，批次最终停靠到完整状态。"""
        with _BATCH_LOCK:
            reconcile = self.get_batch_reconcile(batch_id)
            if reconcile is None:
                return {"ok": False, "error": f"批次「{batch_id}」不存在"}
            if reconcile.get("publishable"):
                # 幂等：批次已完整，重复重试只返回原回执
                return {
                    "ok": True,
                    "batch_id": batch_id,
                    "idempotent": True,
                    "receipt": self._build_receipt(reconcile),
                }

            records = [
                r for r in store.rows("sink_output")
                if r.get("batch_id") == batch_id
            ]
            if not records:
                return {"ok": False, "error": f"批次「{batch_id}」没有可重试的落库数据"}

            retried = 0
            for record in records:
                if record.get("write_status") == "failed":
                    record["write_status"] = "success"
                    record.pop("异常标记", None)
                    record["retried"] = True
                    retried += 1

            failed = sum(1 for r in records if r.get("write_status") == "failed")
            success = len(records) - failed
            publishable = failed == 0

            # 整批写入完成后冻结检查点，并刷新快照中的写入计数
            cp = self._find_checkpoint(reconcile.get("checkpoint_id", ""))
            if cp is not None and publishable:
                states = dict(cp.get("operator_states") or {})
                states["written"] = success
                cp["operator_states"] = states
                cp["snapshot_ts"] = time.time()
                cp["frozen"] = True

            reconcile["成功记录"] = success
            reconcile["失败分区"] = "无" if publishable else reconcile["失败分区"]
            reconcile["可发布"] = "是" if publishable else "否"
            reconcile["publishable"] = publishable
            reconcile["status"] = "可发布" if publishable else "写入失败"
            reconcile["pending"] = not publishable
            reconcile["abnormal"] = not publishable

            # 页面状态最终停靠到一个完整批次状态
            task = self._find_task(reconcile.get("任务名称", ""))
            if task is not None and publishable:
                task["status"] = "运行中"
                task["pending"] = False

            return {
                "ok": True,
                "batch_id": batch_id,
                "retried": retried,
                "receipt": self._build_receipt(reconcile),
            }

    def _build_receipt(self, reconcile: dict[str, Any]) -> dict[str, Any]:
        """统一回执：读取数、落库数、检查点与输出完整性报告。"""
        batch_id = reconcile.get("batch_id") or reconcile.get("批次号", "")
        cp = self._find_checkpoint(reconcile.get("checkpoint_id", ""))
        completeness = SinkOutputService().batch_completeness_report(batch_id)
        return {
            "batch_id": batch_id,
            "task": reconcile.get("任务名称", ""),
            "read_count": reconcile.get("read_count", completeness.get("actual", 0)),
            "persisted": completeness.get("success", 0),
            "failed": completeness.get("failed", 0),
            "checkpoint_id": reconcile.get("checkpoint_id", ""),
            "checkpoint_frozen": bool((cp or {}).get("frozen")),
            "publishable": bool(reconcile.get("publishable")),
            "version": reconcile.get("version", 1),
            "completeness": completeness,
        }

    # ---- 序号34：迁移切换（幂等 + 事务失步回滚） ----

    @staticmethod
    def _current_storage_version() -> int:
        versions = [
            int(b.get("version", 1)) for b in store.rows(RECONCILE_MODULE)
        ]
        versions += [
            int(e.get("to_version", 1)) for e in store.rows(MIGRATION_LEDGER)
        ]
        return max(versions, default=1)

    def _find_desync_batches(self, batches: list[dict[str, Any]]) -> list[str]:
        """事务失步检测：检查点缺失或落库成功数与对账结果不一致。"""
        desync = []
        for b in batches:
            cp_id = b.get("checkpoint_id", "")
            if not cp_id:
                continue  # 未关联检查点的展示数据不参与事务校验
            cp = self._find_checkpoint(cp_id)
            actual = sum(
                1 for r in store.rows("sink_output")
                if r.get("batch_id") == b.get("batch_id")
                and r.get("write_status") == "success"
            )
            if cp is None or actual != int(b.get("成功记录", 0)):
                desync.append(b.get("batch_id") or b.get("批次号", ""))
        return desync

    def apply_migration_switch(
        self, switch_id: str = DEFAULT_SWITCH_ID, task_name: str | None = None,
    ) -> dict[str, Any]:
        """序号34迁移切换：并发/重复切换只生效一次，事务失步回滚原版本。

        切换成功后批次对账结果进入新的存量版本，迁移台账记录 from/to 版本，
        供部署页面与结果落地数据流核对迁移前后结果。
        """
        with _SWITCH_LOCK:
            ledger = store.rows(MIGRATION_LEDGER)
            for entry in ledger:
                if entry.get("switch_id") == switch_id:
                    # 幂等：重复切换只生效一次，返回首次切换结果
                    return {
                        "ok": entry.get("status") == "已生效",
                        "applied": False,
                        "idempotent": True,
                        "entry": dict(entry),
                    }

            batches = [
                b for b in store.rows(RECONCILE_MODULE)
                if task_name is None or b.get("任务名称") == task_name
            ]
            from_version = max(
                (int(b.get("version", 1)) for b in batches), default=1,
            )
            to_version = from_version + 1

            # 先 tentative 切换版本，再校验事务一致性，失步则回滚原版本
            bumped: list[tuple[dict[str, Any], int]] = []
            for b in batches:
                bumped.append((b, int(b.get("version", 1))))
                b["version"] = to_version
                b["存量版本"] = str(to_version)

            desync = self._find_desync_batches(batches)
            if desync:
                for b, old_version in bumped:
                    b["version"] = old_version
                    b["存量版本"] = str(old_version)
                    bid = b.get("batch_id") or b.get("批次号", "")
                    if bid in desync:
                        b["status"] = "已回滚"
                        b["可发布"] = "否"
                        b["publishable"] = False
                entry = {
                    "id": self._next_id(MIGRATION_LEDGER),
                    "switch_id": switch_id,
                    "status": "已回滚",
                    "from_version": from_version,
                    "to_version": from_version,
                    "desync_batches": desync,
                    "reason": "事务失步，回滚原版本",
                    "created_at": time.time(),
                }
                ledger.append(entry)
                return {
                    "ok": False,
                    "applied": False,
                    "rolled_back": True,
                    "entry": dict(entry),
                }

            entry = {
                "id": self._next_id(MIGRATION_LEDGER),
                "switch_id": switch_id,
                "status": "已生效",
                "from_version": from_version,
                "to_version": to_version,
                "batch_count": len(batches),
                "created_at": time.time(),
            }
            ledger.append(entry)
            return {
                "ok": True,
                "applied": True,
                "rolled_back": False,
                "entry": dict(entry),
            }

    def migration_ledger(self) -> list[dict[str, Any]]:
        """迁移台账：供其他页面同步查询迁移前后版本。"""
        return [dict(e) for e in store.rows(MIGRATION_LEDGER)]

    # ---- 动作：复核人员在聚合任务页面发起写入 ----

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起写入":
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            task_name = entry.get("任务名称", f"task_{entry_id}")
            records = self.simulate_batch_records(task_name)
            if records is None:
                return None, f"聚合任务「{task_name}」无法生成聚合结果"
            result = self.flush_batch_with_receipt(task_name, records)
            if not result.get("ok"):
                return None, result.get("error", "批次写入失败")
            receipt = result["receipt"]
            return entry, (
                f"批次 {result['batch_id']} 写入完成：成功 {receipt['persisted']} 条，"
                f"失败 {receipt['failed']} 条，检查点 {receipt['checkpoint_id']}，"
                f"可发布={'是' if receipt['publishable'] else '否'}"
            )
        return super().run_action(entry_id, action)

    # ---- 既有能力 ----

    def validate_sink_task(self, task_name: str) -> dict[str, Any]:
        """校验聚合落库任务的配置完整性。"""
        task = self._find_task(task_name)
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        checks = []
        granularity = task.get("聚合粒度", "")
        valid_granularities = {"minute", "hour", "day", "week", "month"}
        checks.append({
            "check": "granularity",
            "ok": granularity.lower() in valid_granularities,
            "detail": granularity,
        })

        try:
            frequency = int(task.get("落库频率", "60"))
            checks.append({
                "check": "frequency",
                "ok": frequency >= 10 and frequency <= 86400,
                "detail": f"{frequency}s",
            })
        except (ValueError, TypeError):
            checks.append({"check": "frequency", "ok": False, "detail": "格式错误"})

        partition_key = task.get("分区键", "")
        checks.append({
            "check": "partition_key",
            "ok": bool(partition_key and partition_key.strip()),
            "detail": partition_key,
        })

        mode = task.get("覆盖模式", "append")
        checks.append({
            "check": "mode",
            "ok": mode in ("append", "overwrite", "merge"),
            "detail": mode,
        })

        return {
            "ok": all(c["ok"] for c in checks),
            "task": task_name,
            "checks": checks,
        }

    def batch_flush_to_target(self, task_name: str, aggregated: list[dict[str, Any]]) -> dict[str, Any]:
        """将聚合结果批量写入目标存储表并同步检查点。"""
        task = self._find_task(task_name)
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        partition_key = task.get("分区键", "dt")
        mode = task.get("覆盖模式", "append")

        written = 0
        for item in aggregated:
            partition_val = time.strftime("%Y%m%d%H")
            sink_record = {
                "id": self._next_id("sink_output"),
                "配置名称": f"agg_{task_name}_{partition_val}",
                "输出类型": "aggregate_batch",
                "目标地址": f"table://{task.get('输出表名', task_name)}",
                "写入模式": mode,
                "分区策略": partition_key,
                "格式转换": "json",
                "status": "已生效",
                "sink": f"agg_{task_name}",
                "write_status": "success",
                "partition": partition_val,
                "data": item,
                "written_at": time.time(),
            }
            store.rows("sink_output").append(sink_record)
            written += 1

        # 同步检查点
        cp_entry = {
            "id": self._next_id("checkpoint_mgr"),
            "配置名称": f"checkpoint_{task_name}_{int(time.time())}",
            "间隔时间": task.get("落库频率", "60"),
            "超时时长": "120",
            "最大并发": "3",
            "对齐策略": "exactly_once",
            "保留数量": 5,
            "外部化路径": f"/data/checkpoints/{task_name}",
            "status": "已启用",
            "checkpoint_id": f"cp_agg_{task_name}_{int(time.time())}",
            "operator_states": {"task": task_name, "written": written},
            "snapshot_ts": time.time(),
        }
        store.rows("checkpoint_mgr").append(cp_entry)

        task["status"] = "运行中"
        task["pending"] = True

        return {
            "ok": True,
            "task": task_name,
            "written": written,
            "checkpoint_id": cp_entry["checkpoint_id"],
        }

    def compute_storage_metrics(self) -> dict[str, Any]:
        """计算存储指标：所有聚合任务的写入统计。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        metrics = {}
        total_records = 0
        for task in tasks:
            name = task.get("任务名称", "")
            count = sum(
                1 for s in store.rows("sink_output")
                if name in str(s.get("sink", ""))
            )
            total_records += count
            metrics[name] = count

        return {
            "ok": True,
            "task_count": len(tasks),
            "total_records": total_records,
            "per_task": metrics,
            "avg_per_task": total_records / max(len(tasks), 1),
        }

    def list_sink_status(self) -> list[dict[str, Any]]:
        """列出所有聚合落库任务的状态及关联检查点信息。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for t in tasks:
            name = t.get("任务名称", "")
            checkpoints = [
                c for c in store.rows("checkpoint_mgr")
                if name in str(c.get("配置名称", ""))
            ]
            result.append({
                "task": name,
                "granularity": t.get("聚合粒度", ""),
                "mode": t.get("覆盖模式", ""),
                "status": t.get("status", ""),
                "checkpoints": len(checkpoints),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["failed"] = sum(1 for r in rows if r.get("status") == "写入失败")
        base["batches"] = len(store.rows(RECONCILE_MODULE))
        base["publishable_batches"] = sum(
            1 for b in store.rows(RECONCILE_MODULE) if b.get("publishable")
        )
        return base
