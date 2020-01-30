package main

import (
	"context"
	"io"
)

type WalRecord struct {
	Key   string
	Value io.Reader
}

type WalRecordId struct {
	Timestamp int64
	Sequence  int32
	Partition int32
}

type walExRecord struct {
	WalRecord
	WalRecordId
	Crc int32
}

type WalWriter struct {
	partitionCount      int32
	topicName           string
	defaultLogBehaviour WalSyncType

	writerChannel chan WalRecord
}

func NewWalWriter(topicName string, partitionCount int32, defaultLogBehaviour WalSyncType) *WalWriter {

	ret := &WalWriter{
		partitionCount:      partitionCount,
		topicName:           topicName,
		defaultLogBehaviour: defaultLogBehaviour,
		writerChannel:       make(chan WalRecord),
	}

	return ret
}

func (w *WalWriter) Write(ctx context.Context, wr *WalRecord) <-chan WalRecordId {
	ret := make(chan WalRecordId)

	return ret
}
