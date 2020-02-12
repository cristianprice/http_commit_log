package main

import (
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
}

//WalPartition wraps the partition writer and a channel to send events to.
type WalPartition struct {
	writerChannel   chan WalRecord
	partitionWriter *WalPartitionWriter
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

	ret.partitions = make([]*WalPartition, partitionCount)

	var i uint32
	for i = 0; i < partitionCount; i++ {
		ret.partitions[i] = &WalPartition{
			partitionWriter: newWalPartitionWriter(path.CurrentPath, i, maxSegmentSize, walSyncType),
			writerChannel:   make(chan WalRecord),
		}
	}

	//We assume nothing panicked so far.
	for i = 0; i < partitionCount; i++ {

	}

	return ret, nil
}

func newWalPartitionWriter(topicDir string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) *WalPartitionWriter {
	filePath := (&Path{CurrentPath: topicDir}).AddUint32(partitionCount)
	os.MkdirAll(filePath.CurrentPath, 644)

	filePath = filePath.AddInt64(time.Now().UnixNano()).AddExtension(".wal")

	wpw, err := NewWalPartitionWriter(filePath.CurrentPath, maxSegmentSize, walSyncType)
	if err != nil {
		panic(err)
	}

	return wpw
}
