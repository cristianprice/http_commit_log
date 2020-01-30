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

type PartitionedWalWriter struct {
	partitionCount      int32
	topicName           string
	defaultLogBehaviour WalSyncType

	writerChannel chan WalRecord
	resultChannel chan WalRecordId
}

type WalWriter interface {
	Write(ctx context.Context, wr *WalRecord) <-chan WalRecordId
}

func NewWalWriter(topicName string, partitionCount int32, defaultLogBehaviour WalSyncType) WalWriter {

	pw := PartitionedWalWriter{
		partitionCount:      partitionCount,
		topicName:           topicName,
		defaultLogBehaviour: defaultLogBehaviour,
		writerChannel:       make(chan WalRecord),
		resultChannel:       make(chan WalRecordId),
	}

	go func() {

	}()

	return WalWriter(&pw)
}

func (w *PartitionedWalWriter) Close() {
	close(w.writerChannel)
}

func (w *PartitionedWalWriter) Write(ctx context.Context, wr *WalRecord) <-chan WalRecordId {
	return w.resultChannel
}
