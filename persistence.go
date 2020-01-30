package main

import (
	"context"
	"fmt"
	"io"
	"os"
)

type WalRecord struct {
	Key   string
	Value io.Reader
}

type WalRecordId struct {
	Timestamp int64
	Sequence  uint32
	Partition int32
}

type walExRecord struct {
	WalRecord
	WalRecordId
	Crc int32
}

type PartitionedWalWriter struct {
	partitionCount      uint32
	topicName           string
	defaultLogBehaviour WalSyncType
	fileDirPath         string

	sequence_counter uint32

	writerChannel chan WalRecord
	resultChannel chan WalRecordId
}

type singlePartitionWalWriter struct {
	parent          *PartitionedWalWriter
	partitionNumber uint32

	writer *io.Writer
}

type WalWriter interface {
	Write(ctx context.Context, wr *WalRecord) <-chan WalRecordId
	Close()
}

func NewSinglePatitionWalWriter(parent *PartitionedWalWriter, partitionNumber uint32) WalWriter {
	err := os.MkdirAll(fmt.Sprintf("%s%c%d", parent.fileDirPath, os.PathSeparator, partitionNumber), os.ModePerm)
	if err != nil {
		panic(err)
	}

	pw := singlePartitionWalWriter{
		parent:          parent,
		partitionNumber: partitionNumber,
	}

	go func() {

	}()

	return WalWriter(&pw)
}

func (w *singlePartitionWalWriter) Write(ctx context.Context, wr *WalRecord) <-chan WalRecordId {
	return nil
}

func (w *singlePartitionWalWriter) Close() {
	w.Close()
}

func NewWalWriter(topicName string, partitionCount uint32, defaultLogBehaviour WalSyncType) WalWriter {

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
