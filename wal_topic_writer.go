package main

import (
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

//WalTopicWriter writes to a topic and handles file swapping and so on.
type WalTopicWriter struct {
	PartitionCount uint32
	Name           string
	maxSegmentSize int64
	partitions     []*WalPartition
	topicChannel   chan *WalRecord

	ctx             context.Context
	cancel          context.CancelFunc
	currentSequence uint32
}

type walRequest struct {
	walRecord *WalExRecord
	respChan  chan error
}

//WalPartition wraps the partition writer and a channel to send events to.
type WalPartition struct {
	writerChannel   chan *walRequest
	partitionWriter *WalPartitionWriter
}

//Close closes topic writer and releases all resources.
func (w *WalTopicWriter) Close() error {
	if w.cancel != nil {
		w.cancel()
	}

	close(w.topicChannel)
	for _, p := range w.partitions {
		close(p.writerChannel)
		err := p.partitionWriter.Close()
		log.Warn("Failed to close partition writer: ", err)
	}

	return nil
}

//WriteWalRecord writes wal records to different partitions.
func (w *WalTopicWriter) WriteWalRecord(r *WalRecord) chan error {
	retChan := make(chan error)

	if w.ctx.Err() != nil {
		go func() {
			retChan <- w.ctx.Err()
		}()

		return retChan
	}

	w.currentSequence++
	go func(seq uint32) {

		crc, err := Crc32([]byte(r.Key))
		if err != nil {
			return nil, err
		}

		partition := (crc % w.PartitionCount)
		pObj := w.partitions[partition]

		wrEx := NewWalExRecord(r, w.currentSequence, time.Now().UnixNano())
		pObj.writerChannel <- &walRequest{wrEx, retChan}

	}(w.currentSequence)

	return retChan
}

func partitionHandler(ctx context.Context, partitionCount uint32, wp *WalPartition) {

	myCtx := context.WithValue(ctx, fmt.Sprint(partitionCount), fmt.Sprint(partitionCount))
	var wrEx *WalExRecord
	var wReq *walRequest

	readChan := wp.writerChannel
	pw := wp.partitionWriter

	select {
	case wReq = <-readChan:

		wrEx = wReq.walRecord
		b, err := wrEx.Bytes()
		if err != nil {

			wReq.respChan <- err
			defer close(wReq.respChan)
			return

		}

		_, err = pw.Write(b)
		if err == ErrSegLimitReached {
			//TODO handle this with file rollover.
		} else if err != nil {
			wReq.respChan <- err
			defer close(wReq.respChan)
			return

		} else {
			err := pw.Flush()
			wReq.respChan <- err
			defer close(wReq.respChan)
			return

		}

	case <-myCtx.Done():
		close(wp.writerChannel)
		wp.partitionWriter.Close()
		return
	}

}

//NewTopicWriter the actual topic writer.
func NewTopicWriter(parentDir Path, name string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) (*WalTopicWriter, error) {

	path := parentDir.Add(name)

	ret := &WalTopicWriter{
		PartitionCount:  partitionCount,
		Name:            name,
		maxSegmentSize:  maxSegmentSize,
		topicChannel:    make(chan *WalRecord),
		currentSequence: 0,
	}

	ret.partitions = make([]*WalPartition, partitionCount)

	var i uint32
	for i = 0; i < partitionCount; i++ {
		ret.partitions[i] = &WalPartition{
			partitionWriter: newWalPartitionWriter(*path, i, maxSegmentSize, walSyncType),
			writerChannel:   make(chan *WalExRecord),
		}
	}

	//We assume nothing panicked so far.
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ret.ctx)
	ret.ctx = ctx
	ret.cancel = cancel

	go func(ctx context.Context, cancel context.CancelFunc) {
		var walRec *WalRecord

		for {
			select {
			case walRec = <-ret.topicChannel:
				ret.WriteWalRecord(walRec)
			case <-ctx.Done():
				ret.Close()
				return
			}
		}
	}(ret.ctx, cancel)

	for i = 0; i < partitionCount; i++ {
		go partitionHandler(ctx, i, ret.partitions[i])
	}

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
