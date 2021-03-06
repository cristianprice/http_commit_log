package main

import (
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type ctxKey string

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
	log.Debug("Closing topic writer.")
	if w.cancel != nil {
		w.cancel()
	}

	log.Debug("Closing topic channel.")
	close(w.topicChannel)
	for idx, p := range w.partitions {
		log.Debug("Closing partition channel: ", idx)
		close(p.writerChannel)
		err := p.partitionWriter.Close()
		if err != nil {
			log.Warn("Failed to close partition writer: ", err)
		}
	}

	return nil
}

//WriteWalRecord writes wal records to different partitions.
// The channel returned gets owned and closed by receiver.
func (w *WalTopicWriter) WriteWalRecord(r *WalRecord) chan error {
	log.Debug("Received object: ", r)
	log.Debug("Creating return channel.")
	retChan := make(chan error)

	if w.ctx.Err() != nil {
		log.Warnln("Context closed: ", w.ctx.Err())
		go func() {
			retChan <- w.ctx.Err()
		}()

		return retChan
	}

	w.currentSequence++
	log.Debug("Increased current sequence: ", w.currentSequence)
	crc, err := Crc32([]byte(r.Key))
	log.Debug("Calculated crc: ", crc)
	if err != nil {
		go func() {
			retChan <- err
		}()
		return retChan
	}

	partition := (crc % w.PartitionCount)
	log.Debug("Selected partition: ", partition)
	pObj := w.partitions[partition]

	wrEx := NewWalExRecord(r, w.currentSequence, time.Now().UnixNano())
	log.Debug("Sending walExRecord to the partition channel: ", wrEx.Record.Key)
	pObj.writerChannel <- &walRequest{wrEx, retChan}
	log.Debug("Sent walExRecord to the partition channel ...")

	return retChan
}

func partitionHandler(ctx context.Context, partitionCount uint32, wp *WalPartition) {

	log.Debug("Starting partition handler. Partition count:", partitionCount)

	myCtx := context.WithValue(ctx, ctxKey(fmt.Sprint(partitionCount)), fmt.Sprint(partitionCount))
	var wrEx *WalExRecord
	var wReq *walRequest

	readChan := wp.writerChannel

	for {

		select {
		case wReq = <-readChan:
			if wReq == nil {
				log.Warn("Nil value sent to topic writer channel.")
				return
			}

			log.Debug("Reading in key: ", wReq.walRecord.Record.Key)

			wrEx = wReq.walRecord
			b, err := wrEx.Bytes()
			if err != nil {
				wReq.respChan <- err
				continue
			}

			writeWalExRecord(wReq.respChan, wp, b)

		case <-myCtx.Done():
			return
		}

	}

}

func writeWalExRecord(respChan chan error, wp *WalPartition, b []byte) {
	pw := wp.partitionWriter

	log.Debug("Writing data to disk ...")
	_, err := pw.Write(b)
	if err == ErrSegLimitReached {
		log.Warn("Error, segement size limit reached.")

		log.Debug("Closing current partition writer.")
		err = wp.partitionWriter.Close()
		if err != nil {
			respChan <- err
			return
		}

		fPath := GenFileName(wp.partitionWriter.DirPath.String())
		maxSegSize := wp.partitionWriter.MaxSegmentSize
		walSyncType := wp.partitionWriter.WalSyncType

		log.Debugf("Creating new partition writer: file: %s, maxSegSize: %d, walSyncType: %s", fPath, maxSegSize, walSyncType)

		wp.partitionWriter, err = NewWalPartitionWriter(fPath, maxSegSize, walSyncType)
		if err != nil {
			respChan <- err
			return
		}

		log.Debug("Trying to write again.")
		writeWalExRecord(respChan, wp, b)

	} else if err != nil {
		respChan <- err
		return

	} else {
		err := pw.Flush()
		log.Debug("Flushing data with setting: ", pw.WalSyncType)
		respChan <- err
		return

	}

}

//NewTopicWriter the actual topic writer.
func NewTopicWriter(parentDir Path, name string, partitionCount uint32, maxSegmentSize int64, walSyncType WalSyncType) (*WalTopicWriter, error) {

	log.Debug("New topic writer.")
	path := parentDir.Add(name)

	log.Debug("Topic data path: ", path)
	ret := &WalTopicWriter{
		PartitionCount:  partitionCount,
		Name:            name,
		maxSegmentSize:  maxSegmentSize,
		topicChannel:    make(chan *WalRecord),
		currentSequence: 0,
	}

	log.Debug("Creating partitions: ", partitionCount)
	ret.partitions = make([]*WalPartition, partitionCount)

	var i uint32
	for i = 0; i < partitionCount; i++ {
		ret.partitions[i] = &WalPartition{
			partitionWriter: newWalPartitionWriter(path, i, maxSegmentSize, walSyncType),
			writerChannel:   make(chan *walRequest),
		}
	}

	//We assume nothing panicked so far.
	log.Debug("Creating background context.")
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	ret.ctx = ctx
	ret.cancel = cancel

	go func(ctx context.Context, cancel context.CancelFunc) {
		var walRec *WalRecord

		log.Debug("Started topic writer routine.")
		for {
			select {
			case walRec = <-ret.topicChannel:
				ret.WriteWalRecord(walRec)
			case <-ctx.Done():
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
