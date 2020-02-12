package main

import (
	"time"
)

//WalTopicWriter writes to a topic and handles file swapping and so on.
type WalTopicWriter struct {
	PartitionCount   uint32
	Name             string
	maxSegmentSize   int64
	partitionWriters []*WalPartitionWriter
	topicChannel     chan WalRecord
}

//Close closes topic writer and releases all resources.
func (w *WalTopicWriter) Close() error {
	return nil
}

//WriteWalRecord writes wal records to different partitions.
func (w *WalTopicWriter) WriteWalRecord(r *WalRecord) error {

	return nil
}

//NewTopicWriter the actual topic writer.
func NewTopicWriter(parentDir string, name string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) (*WalTopicWriter, error) {

	path := (&Path{CurrentPath: parentDir}).Add(name)

	ret := &WalTopicWriter{
		PartitionCount: partitionCount,
		Name:           name,
		maxSegmentSize: maxSegmentSize,
	}

	ret.partitionWriters = make([]*WalPartitionWriter, partitionCount)

	var i uint32
	for i = 0; i < partitionCount; i++ {
		ret.partitionWriters[i] = createNewTopicWriter(path.CurrentPath, partitionCount, maxSegmentSize, walSyncType)
	}

	return ret, nil
}

func createNewTopicWriter(topicDir string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) *WalPartitionWriter {
	filePath := (&Path{CurrentPath: topicDir}).AddUint32(partitionCount).AddInt64(time.Now().UnixNano()).AddExtension(".wal")
	wpw, err := NewWalPartitionWriter(filePath.CurrentPath, maxSegmentSize, walSyncType)
	if err != nil {
		panic(err)
	}

	return wpw
}
