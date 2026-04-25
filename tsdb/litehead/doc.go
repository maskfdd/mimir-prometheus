// Package litehead 实现了一个轻量级的 Head，只负责写入，不提供查询能力。
//
// 与 tsdb/head 相比，本包有意省略了所有查询相关结构（postings、memSeries
// 的 mmappedChunks 链、查询迭代器、txRing 等）。稳态下每条 series 只保留
// 写入所必需的最小状态：ref、labels 在 arena 中的位置、lastTs、当前正在写
// 的 open chunk；sealed chunk 会立即被 mmap 到磁盘。
//
// 生命周期：
//
//	Open  -> 打开 WAL + ChunkDiskMapper，回放 WAL 恢复 ref/lastTs 映射
//	Append/Commit -> 追加样本到 open chunk、写 WAL，必要时切 chunk 并 spill 到磁盘
//	Flush -> 将一个时间窗口内的样本整理成 TSDB block 并删除已被覆盖的 WAL 段
//	Close -> Checkpoint + 持久化快照，下次启动可快速恢复
//
// 详细设计见 tsdb/docs/write_only_head_design.md。
package litehead
