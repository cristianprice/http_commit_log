package main

//WalTopicWriter writes to a topic and handles file swapping and so on.
type WalTopicWriter struct {
	PartitionCount   uint32
	Name             string
	partitionWriters []*WalPartitionWriter
	topicChannel     chan WalRecord
}

//NewTopicWriter the actual topic writer.
func NewTopicWriter(topicDir string, name string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) (*WalTopicWriter, error) {
	ret := &WalTopicWriter{
		PartitionCount: partitionCount,
		Name:           name,
	}

	ret.partitionWriters = make([]*WalPartitionWriter, partitionCount)

	var i uint32
	for i = 0; i < partitionCount; i++ {
		ret.partitionWriters[i] = createNewTopicWriter(topicDir, partitionCount, maxSegmentSize, walSyncType)
	}

	return ret, nil
}

func createNewTopicWriter(topicDir string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) *WalPartitionWriter {
	return nil
}
