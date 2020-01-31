package main

import (
	"context"
	"io"
)

type WalRecord struct {
	Key         string
	SizeOfValue uint32
	Value       io.Reader
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

type WalWriter interface {
	Write(ctx context.Context, wr *WalRecord) <-chan WalRecordId
	Close()
}
