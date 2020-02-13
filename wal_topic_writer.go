package main

import (
	"context"
	"os"
	"time"
)

//WalTopicWriter writes to a topic and handles file swapping and so on.
type WalTopicWriter struct {
	PartitionCount uint32
	Name           string
	maxSegmentSize int64
	partitions     []*WalPartition
	topicChannel   chan WalRecord

	ctx    context.Context
	cancel context.CancelFunc
}

//WalPartition wraps the partition writer and a channel to send events to.
type WalPartition struct {
	writerChannel   chan WalRecord
	partitionWriter *WalPartitionWriter
}

//Close closes topic writer and releases all resources.
func (w *WalTopicWriter) Close() error {
	if w.cancel != nil {
		w.cancel()
	}

	return nil
}

//WriteWalRecord writes wal records to different partitions.
func (w *WalTopicWriter) WriteWalRecord(r *WalRecord) error {
	return nil
}

//NewTopicWriter the actual topic writer.
func NewTopicWriter(parentDir Path, name string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) (*WalTopicWriter, error) {

	path := parentDir.Add(name)

	ret := &WalTopicWriter{
		PartitionCount: partitionCount,
		Name:           name,
		maxSegmentSize: maxSegmentSize,
		topicChannel:   make(chan WalRecord),
	}

	ret.partitions = make([]*WalPartition, partitionCount)

	var i uint32
	for i = 0; i < partitionCount; i++ {
		ret.partitions[i] = &WalPartition{
			partitionWriter: newWalPartitionWriter(*path, i, maxSegmentSize, walSyncType),
			writerChannel:   make(chan WalRecord),
		}
	}

	//We assume nothing panicked so far.
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ret.ctx)
	ret.ctx = ctx
	ret.cancel = cancel

	go func(ctx context.Context, cancel context.CancelFunc) {
		var walRec WalRecord

		for {
			select {
			case walRec = <-ret.topicChannel:
				println(walRec)
			case <-ctx.Done():
				return
			}
		}
	}(ret.ctx, cancel)
	return ret, nil
}

func newWalPartitionWriter(topicDir Path, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) *WalPartitionWriter {
	filePath := topicDir.AddUint32(partitionCount)
	os.MkdirAll(filePath.String(), 644)

	filePath = filePath.AddInt64(time.Now().UnixNano()).AddExtension(".wal")

	wpw, err := NewWalPartitionWriter(filePath.String(), maxSegmentSize, walSyncType)
	if err != nil {
		panic(err)
	}

	return wpw
}
