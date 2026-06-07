package litehead

import "github.com/prometheus/client_golang/prometheus"

// headMetrics 汇总 Head 暴露的监控指标。
// 指标名对齐标准 tsdb.Head（prometheus_tsdb_head_* / wal / checkpoint），
// litehead 特有指标使用 prometheus_tsdb_litehead_* 前缀。
type headMetrics struct {
	r prometheus.Registerer

	// Head 指标（prometheus_tsdb_head_*）
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

	// WAL / checkpoint 指标
	walCorruptionsTotal     prometheus.Counter
	walReplayDuration       prometheus.Gauge
	walTruncateDuration     prometheus.Summary
	checkpointCreationTotal prometheus.Counter
	checkpointCreationFail  prometheus.Counter

	// litehead 特有指标（prometheus_tsdb_litehead_*）
	mmappedChunksForcedFlush prometheus.Counter
	// mmappedChunksSoftFlushHits 每次某条 series 的 sealed 数突破软阈值但未触达
	// 硬上限时 +1。**该指标上升**说明外部 flush 节奏跟不上，但系统仍在安全工作；
	// 一旦它与 forced flush 指标的比值长期偏低并且绝对值陡升，就是需要调低
	// FlushCheckInterval 的信号。
	mmappedChunksSoftFlushHits prometheus.Counter
	// mmappedChunksHardLimit 是当前 Head 配置的 sealed chunks 硬上限（常量 gauge），
	// 用于让监控侧把 forced flush 次数与当前阈值对齐，而不是靠预期常量。
	mmappedChunksHardLimit prometheus.Gauge
	// mmappedChunksSoftLimit 是当前 Head 配置的 sealed chunks 软告警阈值。
	mmappedChunksSoftLimit   prometheus.Gauge
	earlyFlushTriggered      prometheus.Counter
	labelCatalogSize         prometheus.Gauge
	labelCatalogCount        prometheus.Gauge
	labelCatalogSymbolsSize  prometheus.Gauge
	labelCatalogSymbolsCount prometheus.Gauge

	snapshotLoadDuration prometheus.Gauge
}

func newHeadMetrics(r prometheus.Registerer) *headMetrics {
	m := &headMetrics{r: r}

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

	// ---- litehead flush-to-block 指标 ----
	// 使用 prometheus_tsdb_litehead_* 前缀，避免与 DB 层的
	// prometheus_tsdb_compactions_* / compaction_duration_seconds 冲突——
	// 两者语义不同：DB 层是磁盘 block 合并，litehead 是内存 flush 成 block。

	m.compactionsTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_litehead_compactions_triggered_total",
		Help: "Total number of triggered flush-to-block compactions for the lite head.",
	})
	m.compactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_litehead_compactions_failed_total",
		Help: "Total number of flush-to-block compactions that failed for the lite head.",
	})
	m.compactionDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "prometheus_tsdb_litehead_compaction_duration_seconds",
		Help: "Duration of lite head flush-to-block compactions.",
	})

	// ---- prometheus_tsdb_wal_* / prometheus_tsdb_checkpoint_* ----

	m.walCorruptionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_corruptions_total",
		Help: "Total number of WAL corruptions.",
	})
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
		Help: "Total number of times a series forced a synchronous flush because its mmapped chunks slot reached the hard limit. This path is an extreme fallback; a non-zero rate usually indicates Flush() is called too infrequently.",
	})
	m.mmappedChunksSoftFlushHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_litehead_mmapped_chunks_soft_flush_hits_total",
		Help: "Total number of times a single series crossed the soft sealed-chunks watermark without forcing a flush. Use it as an early warning that Flush() cadence is slower than the series's chunk churn.",
	})
	m.mmappedChunksHardLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_mmapped_chunks_hard_limit",
		Help: "Configured hard upper bound of sealed mmapped chunks a single series may hold before triggering a forced flush.",
	})
	m.mmappedChunksSoftLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_mmapped_chunks_soft_limit",
		Help: "Configured soft watermark of sealed mmapped chunks per series. Crossing it only increments soft_flush_hits, never forces a flush.",
	})
	m.earlyFlushTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_litehead_early_flush_triggered_total",
		Help: "Total number of times early flush was triggered because active series count exceeded EarlyFlushMinSeries threshold.",
	})
	m.labelCatalogSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_label_catalog_bytes",
		Help: "Approximate size of the lite head labels arena in bytes.",
	})
	m.labelCatalogCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_label_catalog_entries",
		Help: "Number of labelsID entries stored in the lite head labels arena.",
	})
	m.labelCatalogSymbolsSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_label_catalog_symbols_bytes",
		Help: "Total bytes of distinct label name/value strings interned by the lite head symbol table.",
	})
	m.labelCatalogSymbolsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_label_catalog_symbols_total",
		Help: "Number of distinct label name/value strings interned by the lite head symbol table.",
	})
	m.snapshotLoadDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_litehead_snapshot_load_duration_seconds",
		Help: "Time taken to load the lite snapshot on startup.",
	})

	if r != nil {
		r.MustRegister(
			m.seriesActive, m.seriesCreated, m.seriesRemoved,
			m.chunksCreated, m.chunksSealed,
			m.samplesAppended, m.outOfOrderSamples,
			m.compactionsTriggered, m.compactionsFailed, m.compactionDuration,
			m.walCorruptionsTotal, m.walReplayDuration, m.walTruncateDuration,
			m.checkpointCreationTotal, m.checkpointCreationFail,
			m.mmappedChunksForcedFlush, m.mmappedChunksSoftFlushHits,
			m.mmappedChunksHardLimit, m.mmappedChunksSoftLimit,
			m.earlyFlushTriggered,
			m.labelCatalogSize, m.labelCatalogCount,
			m.labelCatalogSymbolsSize, m.labelCatalogSymbolsCount,
			m.snapshotLoadDuration,
		)
	}
	return m
}

func (m *headMetrics) unregister() {
	if m == nil || m.r == nil {
		return
	}
	for _, c := range []prometheus.Collector{
		m.seriesActive, m.seriesCreated, m.seriesRemoved,
		m.chunksCreated, m.chunksSealed,
		m.samplesAppended, m.outOfOrderSamples,
		m.compactionsTriggered, m.compactionsFailed, m.compactionDuration,
		m.walCorruptionsTotal, m.walReplayDuration, m.walTruncateDuration,
		m.checkpointCreationTotal, m.checkpointCreationFail,
		m.mmappedChunksForcedFlush, m.mmappedChunksSoftFlushHits,
		m.mmappedChunksHardLimit, m.mmappedChunksSoftLimit,
		m.earlyFlushTriggered,
		m.labelCatalogSize, m.labelCatalogCount,
		m.labelCatalogSymbolsSize, m.labelCatalogSymbolsCount,
		m.snapshotLoadDuration,
	} {
		m.r.Unregister(c)
	}
}
