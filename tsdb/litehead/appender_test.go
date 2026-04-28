package litehead

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
)

// TestUnsupportedWriteTypesReturnExplicitError 验证 litehead 对 exemplar / histogram /
// metadata 的写入调用一律显式返回 ErrUnsupportedWriteType，而不是静默成功。
//
// 这是 PR-1 的核心语义保证：接入方一旦错误地调用到这些入口，应该立刻感知，
// 而不是像之前那样拿到 (0, nil) 误以为写入成功。
func TestUnsupportedWriteTypesReturnExplicitError(t *testing.T) {
	h, _ := newTestHead(t, nil)
	t.Cleanup(func() { _ = h.Close() })

	ctx := context.Background()
	lset := labels.FromStrings("__name__", "cpu")

	app := h.Appender(ctx)

	// Exemplar 写入必须显式失败。
	_, err := app.AppendExemplar(0, lset, exemplar.Exemplar{Labels: lset, Value: 1, Ts: 1000})
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Integer histogram 写入必须显式失败。
	_, err = app.AppendHistogram(0, lset, 1000, &histogram.Histogram{}, nil)
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Float histogram 写入必须显式失败（同入口，float 分支走 nil int hist）。
	_, err = app.AppendHistogram(0, lset, 1000, nil, &histogram.FloatHistogram{})
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Metadata 写入必须显式失败。
	_, err = app.UpdateMetadata(0, lset, metadata.Metadata{Type: "counter"})
	require.ErrorIs(t, err, ErrUnsupportedWriteType)

	// Commit 不应因为此前的 unsupported 错误而失败：appender 未记录任何 pending 状态。
	require.NoError(t, app.Commit())
}
