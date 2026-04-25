// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package litehead

import "github.com/prometheus/client_golang/prometheus"

// dbMetrics 汇总 LiteHead 暴露给用户的监控指标。
//
// 指标名、字段名均对齐标准 tsdb.Head 的 headMetrics / walMetrics /
// checkpointMetrics，方便 mimir-ingester 替换时现有 Grafana 面板和告警规则
// 无需修改：
//
//   - prometheus_tsdb_head_*         : Head 本身（series / chunks / samples / compactions）
//   - prometheus_tsdb_wal_*          : WAL truncate
//   - prometheus_tsdb_checkpoint_*   : Checkpoint 创建
//
// litehead 特有的少数指标保留 prometheus_tsdb_litehead_* 前缀（labelCatalog、
// mmappedChunksForcedFlush），因为标准 Head 里没有对应物。
type dbMetrics struct {
	r prometheus.Registerer

	// ===== 对齐 tsdb.Head 的指标（prometheus_tsdb_head_*）=====

	seriesActive      prometheus.Gauge
	seriesCreated     prometheus.Counter
	seriesRemoved     prometheus.Counter
	chunksCreated     prometheus.Counter
	chunksSealed      prometheus.Counter
	samplesAppended   prometheus.Counter
	outOfOrderSamples prometheus.Counter

	compactionsTriggered prometheus.Counter
	compactionsFailed    prometheus.Counter
	compactionDuration   prometheus.Summary

	// ===== 对齐 WAL / checkpoint 的指标 =====

	walReplayDuration       prometheus.Gauge
	walTruncateDuration     prometheus.Summary
	checkpointCreationTotal prometheus.Counter
	checkpointCreationFail  prometheus.Counter

	// ===== litehead 特有指标（prometheus_tsdb_litehead_*）=====

	mmappedChunksForcedFlush prometheus.Counter
	labelCatalogSize         prometheus.Gauge
	labelCatalogCount        prometheus.Gauge
}

func newDBMetrics(r prometheus.Registerer) *dbMetrics {
	m := &dbMetrics{r: r}

	// ---- prometheus_tsdb_head_* ----

	m.seriesActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_series",
		Help: "Total number of series in the head block.",
	})
	m.seriesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_created_total",
		Help: "Total number of series created in the head.",
	})
	m.seriesRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_removed_total",
		Help: "Total number of series removed in the head.",
	})
	m.chunksCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_chunks_created_total",
		Help: "Total number of chunks created in the head.",
	})
	m.chunksSealed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_chunks_removed_total",
		Help: "Total number of chunks sealed and mmapped out of the head.",
	})
	m.samplesAppended = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_samples_appended_total",
		Help: "Total number of appended samples.",
	})
	m.outOfOrderSamples = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_out_of_order_samples_appended_total",
		Help: "Total number of out-of-order samples rejected.",
	})

	// ---- prometheus_tsdb_compactions_* 这组指标在标准 DB 里，Head 里没有；
	// 这里沿用 mimir/tsdb.DB 的命名，方便统一观测 compact 行为。----

	m.compactionsTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_triggered_total",
		Help: "Total number of triggered compactions for the lite head.",
	})
	m.compactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_failed_total",
		Help: "Total number of compactions that failed for the lite head.",
	})
	m.compactionDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "prometheus_tsdb_compaction_duration_seconds",
		Help: "Duration of lite head compactions (producing a block).",
	})

	// ---- prometheus_tsdb_wal_* / prometheus_tsdb_checkpoint_* ----

	m.walReplayDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_data_replay_duration_seconds",
		Help: "Time taken to replay the data on disk.",
	})
	m.walTruncateDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "prometheus_tsdb_wal_truncate_duration_seconds",
		Help: "Duration of WAL truncation.",
	})
	m.checkpointCreationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_total",
		Help: "Total number of checkpoint creations attempted.",
	})
	m.checkpointCreationFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_failed_total",
		Help: "Total number of checkpoint creations that failed.",
	})

	// ---- litehead 特有指标 ----

	m.mmappedChunksForcedFlush = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_litehead_mmapped_chunks_forced_flush_total",
		Help: "Total number of times a series forced a synchronous flush because its mmapped chunks slot was full.",
	})
	m.labelCatalogSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_label_catalog_bytes",
		Help: "Approximate size of the lite head labels arena in bytes.",
	})
	m.labelCatalogCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_label_catalog_entries",
		Help: "Number of labelsID entries stored in the lite head labels arena.",
	})

	if r != nil {
		r.MustRegister(
			m.seriesActive, m.seriesCreated, m.seriesRemoved,
			m.chunksCreated, m.chunksSealed,
			m.samplesAppended, m.outOfOrderSamples,
			m.compactionsTriggered, m.compactionsFailed, m.compactionDuration,
			m.walReplayDuration, m.walTruncateDuration,
			m.checkpointCreationTotal, m.checkpointCreationFail,
			m.mmappedChunksForcedFlush, m.labelCatalogSize, m.labelCatalogCount,
		)
	}
	return m
}

func (m *dbMetrics) unregister() {
	if m == nil || m.r == nil {
		return
	}
	for _, c := range []prometheus.Collector{
		m.seriesActive, m.seriesCreated, m.seriesRemoved,
		m.chunksCreated, m.chunksSealed,
		m.samplesAppended, m.outOfOrderSamples,
		m.compactionsTriggered, m.compactionsFailed, m.compactionDuration,
		m.walReplayDuration, m.walTruncateDuration,
		m.checkpointCreationTotal, m.checkpointCreationFail,
		m.mmappedChunksForcedFlush, m.labelCatalogSize, m.labelCatalogCount,
	} {
		m.r.Unregister(c)
	}
}
