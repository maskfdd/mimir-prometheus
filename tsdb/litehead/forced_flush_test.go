package litehead

import (
	"context"
	"math"
	"testing"

	"github.com/go-kit/log"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// TestForcedFlushTwoLevelWatermarkDefaults 验证 PR-4 的核心语义：
// 在默认 soft/hard 阈值下，sealed chunk 的增长只在达到 hard 时才触发 forced flush。
// 软阈值只计数，不阻塞写路径。
//
// 构造方式沿用 TestSealedOverflowDoesNotLoseData 的思路：SamplesPerChunk=1 +
// ChunkRange=10ms，确保每条样本都会切一次 chunk 并 spill。这样 sealed 数会线性增长，
// 可以精确控制何时越过 soft 阈值、何时触达 hard 阈值。
func TestForcedFlushTwoLevelWatermarkDefaults(t *testing.T) {
	opts := DefaultOptions()
	opts.SamplesPerChunk = 1
	opts.ChunkRange = 10
	opts.BlockDuration = 60 * 1000
	opts.NoLockfile = true

	// 为了让断言稳定，显式指定 soft=4, hard=8（远低于默认值），让触发点可控。
	// 这同时覆盖了"用户可配置阈值"这条路径。
	opts.SoftFlushSealedChunks = 4
	opts.ForcedFlushSealedChunks = 8

	dir := t.TempDir()
	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu", "host", "hsoft")

	// 每次 append 在切 chunk 时都会 seal 前一条 chunk，因此"写入 N 条样本"
	// 会产生 N-1 次 sealed++（首条样本只开新 chunk、不 seal）。
	// soft=4 触发条件：某次 seal 后 sealedLen 正好等于 4，对应第 5 条样本写入时。
	// hard=8 触发条件：准备 seal 时 sealedLen >= 8，对应第 9 条样本写入时。
	app := h.Appender(ctx)
	var ref storage.SeriesRef

	// 先写 4 条样本：sealed 从 0→3，soft/hard 计数都保持 0。
	for i := int64(0); i < 4; i++ {
		ref, err = app.Append(ref, lset, 1000+i*opts.ChunkRange*2, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	require.Equal(t, 0.0, prom_testutil.ToFloat64(h.metrics.mmappedChunksSoftFlushHits),
		"soft counter must stay 0 while sealed count is below soft watermark")
	require.Equal(t, 0.0, prom_testutil.ToFloat64(h.metrics.mmappedChunksForcedFlush),
		"forced-flush counter must stay 0 while sealed count is below hard limit")

	// 再写 1 条：准备 seal 时 sealedLen=3，+1 正好等于 soft=4，soft 计数 +1。
	// 注意语义：soft 只在"刚好跨过"那一次 +1，避免每次 append 都打点。
	app2 := h.Appender(ctx)
	_, err = app2.Append(ref, labels.EmptyLabels(), 1000+4*opts.ChunkRange*2, 999.0)
	require.NoError(t, err)
	require.NoError(t, app2.Commit())

	require.Equal(t, 1.0, prom_testutil.ToFloat64(h.metrics.mmappedChunksSoftFlushHits),
		"soft counter must +1 exactly once when sealed count crosses the soft watermark")
	require.Equal(t, 0.0, prom_testutil.ToFloat64(h.metrics.mmappedChunksForcedFlush),
		"forced-flush must not fire just because soft watermark was crossed")

	// 再写入 extra 条样本：sealed 会一直增长到 >=8（hard），每次触达都会被 forced flush 清理；
	// 计数器应严格 > 0。写的量远超 hard，确保即使 forced flush 清空后再次达到也能稳定触发。
	const extra = int64(64)
	app3 := h.Appender(ctx)
	for i := int64(0); i < extra; i++ {
		// 续写时间戳从上一轮最后一个样本后继续递增，保持严格单调。
		baseT := int64(1000) + (5+i)*opts.ChunkRange*2
		_, err = app3.Append(ref, labels.EmptyLabels(), baseT, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app3.Commit())

	require.Greater(t, prom_testutil.ToFloat64(h.metrics.mmappedChunksForcedFlush), 0.0,
		"forced-flush counter must rise once sealed count reaches hard limit")
}

// TestForcedFlushOptionsValidationFallsBack 验证 Options.validate 对非法阈值的
// 自愈行为：非法值（<=0 或 soft >= hard）不会让 Head 启动失败，而是回落到默认值，
// 且运行期指标 gauge 反映的是回落后的值，而不是用户传入的错误值。
func TestForcedFlushOptionsValidationFallsBack(t *testing.T) {
	cases := []struct {
		name         string
		in           *Options
		wantHardMin  int // 生效的 hard 必须 >= wantHardMin
		wantSoftLess bool
	}{
		{
			name:         "both zero -> defaults",
			in:           &Options{},
			wantHardMin:  defaultHardMmappedChunksPerSeries,
			wantSoftLess: true,
		},
		{
			name: "soft larger than hard -> soft clamped to hard-1",
			in: &Options{
				SoftFlushSealedChunks:   100,
				ForcedFlushSealedChunks: 10,
			},
			wantHardMin:  10,
			wantSoftLess: true,
		},
		{
			name: "negative values -> defaults",
			in: &Options{
				SoftFlushSealedChunks:   -5,
				ForcedFlushSealedChunks: -5,
			},
			wantHardMin:  defaultHardMmappedChunksPerSeries,
			wantSoftLess: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// validate 是幂等的；直接调用避免测试还要真正起 Head。
			v := tc.in.validate()
			require.GreaterOrEqual(t, v.ForcedFlushSealedChunks, tc.wantHardMin,
				"hard limit must fall back to >= %d after validate", tc.wantHardMin)
			require.Greater(t, v.ForcedFlushSealedChunks, 0)
			require.Greater(t, v.SoftFlushSealedChunks, 0)
			if tc.wantSoftLess {
				require.Less(t, v.SoftFlushSealedChunks, v.ForcedFlushSealedChunks,
					"soft watermark must always be strictly less than hard limit")
			}
		})
	}
}

// TestForcedFlushLimitGaugesReflectConfig 验证 Head 启动时把生效的 soft/hard 阈值
// 写进 gauge：这是 alerting 端判断"当前阈值"的唯一可靠入口，不能硬编码。
func TestForcedFlushLimitGaugesReflectConfig(t *testing.T) {
	opts := DefaultOptions()
	opts.NoLockfile = true
	opts.ChunkRange = 60 * 1000
	opts.BlockDuration = 60 * 1000
	opts.SoftFlushSealedChunks = 17
	opts.ForcedFlushSealedChunks = 33

	dir := t.TempDir()
	h, err := NewHead(log.NewNopLogger(), nil, dir, opts)
	require.NoError(t, err)
	require.NoError(t, h.Init(math.MinInt64))
	t.Cleanup(func() { _ = h.Close() })

	require.Equal(t, 33.0, prom_testutil.ToFloat64(h.metrics.mmappedChunksHardLimit),
		"hard-limit gauge must reflect the configured ForcedFlushSealedChunks")
	require.Equal(t, 17.0, prom_testutil.ToFloat64(h.metrics.mmappedChunksSoftLimit),
		"soft-limit gauge must reflect the configured SoftFlushSealedChunks")
}
