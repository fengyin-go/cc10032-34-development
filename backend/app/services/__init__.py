"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.stream_source import StreamSourceService
from app.services.window_config import WindowConfigService
from app.services.calc_operator import CalcOperatorService
from app.services.state_store import StateStoreService
from app.services.watermark_track import WatermarkTrackService
from app.services.alert_policy import AlertPolicyService
from app.services.sliding_index import SlidingIndexService
from app.services.join_stream import JoinStreamService
from app.services.sink_output import SinkOutputService
from app.services.backfill_job import BackfillJobService
from app.services.cep_rule import CepRuleService
from app.services.throughput_mgr import ThroughputMgrService
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.restore_plan import RestorePlanService
from app.services.metric_lineage import MetricLineageService
from app.services.late_handler import LateHandlerService
from app.services.task_topology import TaskTopologyService
from app.services.data_sampler import DataSamplerService
from app.services.debug_trace import DebugTraceService
from app.services.aggregate_sink import AggregateSinkService
from app.services.batch_reconcile import BatchReconcileService
from app.services.stream_job import StreamJobService
from app.services.anomaly_detector import AnomalyDetectorService
from app.services.replay_engine import ReplayEngineService

SERVICES: dict[str, BaseModuleService] = {
    "stream_source": StreamSourceService(),
    "window_config": WindowConfigService(),
    "calc_operator": CalcOperatorService(),
    "state_store": StateStoreService(),
    "watermark_track": WatermarkTrackService(),
    "alert_policy": AlertPolicyService(),
    "sliding_index": SlidingIndexService(),
    "join_stream": JoinStreamService(),
    "sink_output": SinkOutputService(),
    "backfill_job": BackfillJobService(),
    "cep_rule": CepRuleService(),
    "throughput_mgr": ThroughputMgrService(),
    "checkpoint_mgr": CheckpointMgrService(),
    "restore_plan": RestorePlanService(),
    "metric_lineage": MetricLineageService(),
    "late_handler": LateHandlerService(),
    "task_topology": TaskTopologyService(),
    "data_sampler": DataSamplerService(),
    "debug_trace": DebugTraceService(),
    "aggregate_sink": AggregateSinkService(),
    "batch_reconcile": BatchReconcileService(),
    "stream_job": StreamJobService(),
    "anomaly_detector": AnomalyDetectorService(),
    "replay_engine": ReplayEngineService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)