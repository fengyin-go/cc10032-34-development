<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">实时计算服务中心</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "流式源端", path: "/stream_source" }, { label: "窗口配置", path: "/window_config" }, { label: "计算算子", path: "/calc_operator" }, { label: "状态存储", path: "/state_store" }, { label: "水位追踪", path: "/watermark_track" }, { label: "告警策略", path: "/alert_policy" }, { label: "滑动指标", path: "/sliding_index" }, { label: "流关联", path: "/join_stream" }, { label: "结果落地", path: "/sink_output" }, { label: "补数任务", path: "/backfill_job" }, { label: "CEP规则", path: "/cep_rule" }, { label: "吞吐管理", path: "/throughput_mgr" }, { label: "检查点", path: "/checkpoint_mgr" }, { label: "故障恢复", path: "/restore_plan" }, { label: "指标血缘", path: "/metric_lineage" }, { label: "迟到处理", path: "/late_handler" }, { label: "任务拓扑", path: "/task_topology" }, { label: "数据采样", path: "/data_sampler" }, { label: "调试追踪", path: "/debug_trace" }, { label: "聚合落库", path: "/aggregate_sink" }, { label: "批次对账", path: "/batch_reconcile" }]
</script>
