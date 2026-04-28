package litehead

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/tsdb/chunks"
)

// TestSealedInlineAndOverflow 验证 memSeries 的 "1 inline + overflow slice"
// 结构在 append / 遍历 / 按位访问 / 就地压缩 这四条路径上的语义与原固定数组等价。
//
// 这是 PR-2（mmappedChunks 瘦身）的结构回归测试：只要下面每个断言都成立，
// 上层 appender / blockreader / flush 的逻辑就可以原样复用辅助方法。
func TestSealedInlineAndOverflow(t *testing.T) {
	s := &memSeries{}

	require.Equal(t, 0, s.sealedLen())
	require.Nil(t, s.sealedAt(0))
	require.Nil(t, s.sealedAt(-1))

	// 追加第 1 条：应落在 inline 槽。
	s.appendSealed(mmappedChunk{ref: chunks.ChunkDiskMapperRef(100), minTime: 0, maxTime: 10})
	require.Equal(t, 1, s.sealedLen())
	require.Equal(t, int64(10), s.sealedAt(0).maxTime)
	require.Equal(t, 0, len(s.sealedOverflow))

	// 追加第 2..5 条：走 overflow。
	for i := 2; i <= 5; i++ {
		s.appendSealed(mmappedChunk{
			ref:     chunks.ChunkDiskMapperRef(100 + uint64(i)),
			minTime: int64(i-1) * 10,
			maxTime: int64(i) * 10,
		})
	}
	require.Equal(t, 5, s.sealedLen())
	require.Equal(t, 4, len(s.sealedOverflow))

	// 顺序访问：依次应返回 inline、overflow[0..]。
	var seen []int64
	s.forEachSealed(func(mc mmappedChunk) {
		seen = append(seen, mc.maxTime)
	})
	require.Equal(t, []int64{10, 20, 30, 40, 50}, seen)

	// 就地压缩：保留 maxTime > 25 的条目（即 30/40/50）。
	var keptMins []int64
	s.retainSealedAfter(25, func(mc mmappedChunk) {
		keptMins = append(keptMins, mc.minTime)
	})
	require.Equal(t, 3, s.sealedLen())
	require.Equal(t, []int64{20, 30, 40}, keptMins)

	// 压缩后顺序应为 30/40/50，且 inline 必须是第 0 条。
	seen = seen[:0]
	s.forEachSealed(func(mc mmappedChunk) {
		seen = append(seen, mc.maxTime)
	})
	require.Equal(t, []int64{30, 40, 50}, seen)
	require.Equal(t, int64(30), s.sealedInline.maxTime)

	// 再次压缩到 0 条：inline 应清零，overflow 应空。
	s.retainSealedAfter(100, nil)
	require.Equal(t, 0, s.sealedLen())
	require.Equal(t, mmappedChunk{}, s.sealedInline)
	require.Equal(t, 0, len(s.sealedOverflow))
}

// TestSealedRetainBoundary 覆盖 retainSealedAfter 的边界：
// 保留全部、保留中间一段、只保留第一条、全部丢弃。
func TestSealedRetainBoundary(t *testing.T) {
	build := func(n int) *memSeries {
		s := &memSeries{}
		for i := 1; i <= n; i++ {
			s.appendSealed(mmappedChunk{
				minTime: int64(i-1) * 10,
				maxTime: int64(i) * 10,
			})
		}
		return s
	}

	// 保留全部。
	s := build(5)
	s.retainSealedAfter(-1, nil)
	require.Equal(t, 5, s.sealedLen())

	// 保留中间一段（maxTime > 20 -> 30/40/50）。
	s = build(5)
	s.retainSealedAfter(20, nil)
	require.Equal(t, 3, s.sealedLen())
	require.Equal(t, int64(30), s.sealedAt(0).maxTime)
	require.Equal(t, int64(50), s.sealedAt(2).maxTime)

	// 只保留最后一条（maxTime > 40 -> 50）。
	s = build(5)
	s.retainSealedAfter(40, nil)
	require.Equal(t, 1, s.sealedLen())
	require.Equal(t, int64(50), s.sealedInline.maxTime)
	require.Equal(t, 0, len(s.sealedOverflow))

	// 全部丢弃。
	s = build(5)
	s.retainSealedAfter(999, nil)
	require.Equal(t, 0, s.sealedLen())
	require.Nil(t, s.sealedAt(0))
}
