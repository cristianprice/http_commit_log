package main

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

//WalPartitionWriter abstraction for individual partition writer.
type WalPartitionWriter struct {
	mutex          sync.Mutex
	MaxSegmentSize int64

	File   *os.File
	Writer *bufio.Writer

	WalSyncType   WalSyncType
	CurrentOffset int64
	DirPath       *Path
}

//NewWalPartitionWriter creates a new WalPartitionWriter
func NewWalPartitionWriter(filePath string, maxSegmentSize int64, walSyncType WalSyncType) (*WalPartitionWriter, error) {
	log.Debugf("Creating new partition writer: filePath:%s, maxSegmentSize: %d, walSyncType: %s", filePath, maxSegmentSize, walSyncType)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	log.Debug("Opened file: ", filePath)
	offset, err := MoveToLastValidWalEntry(file, maxSegmentSize)
	if err != nil {
		return nil, err
	}

	log.Debug("Moved offset file to: ", offset)
	dirPath := Path(filePath).BaseDir()

	ret := &WalPartitionWriter{
		File:           file,
		Writer:         bufio.NewWriter(file),
		CurrentOffset:  offset,
		WalSyncType:    walSyncType,
		DirPath:        &dirPath,
		MaxSegmentSize: maxSegmentSize,
	}

	return ret, nil
}

func (w *WalPartitionWriter) Write(p []byte) (n int, err error) {
	log.Debug("Locking for writing.")

	w.mutex.Lock()
	defer w.mutex.Unlock()
	log.Debug("Locking done.")

	if w.CurrentOffset > w.MaxSegmentSize {
		log.Warn("Segment size limit has been reached.")
		return -1, ErrSegLimitReached
	}

	size := []byte{0, 0, 0, 0}
	binary.LittleEndian.PutUint32(size, uint32(binary.Size(p)))

	log.Debug("Packet size to write: ", binary.Size(p))
	ret := 0

	count, err := w.Writer.Write(size)
	if err != nil {
		return -1, err
	}

	ret += count
	count, err = w.Writer.Write(p)

	if count == binary.Size(p) {
		log.Debug("Written bytes: ", count)
	} else {
		log.Warnf("Was supposed to write: %d but wrote: %d bytes.", binary.Size(p), count)
	}

	if err != nil {
		return -1, err
	}

	ret += count
	w.CurrentOffset += int64(ret)

	log.Debug("Incremented offset. Current offset: ", w.CurrentOffset)
	return ret, nil
}

//Flush flushes data to file handle based on options
func (w *WalPartitionWriter) Flush() error {

	log.Debug("Locking ...")
	w.mutex.Lock()
	defer w.mutex.Unlock()

	log.Debug("Flushing to disk.")
	err := w.Writer.Flush()
	if err != nil {
		log.Warn("Failed to flush to file: ", w.File, " ", err)
	}

	if w.WalSyncType == FlushOnCommit {
		log.Debug("Syncing to disk.")
		err = w.File.Sync()
	}

	return err
}

//Close closes the underlaying file handle.
func (w *WalPartitionWriter) Close() error {
	log.Debug("Closing writer.")
	w.Flush()

	w.mutex.Lock()
	defer w.mutex.Unlock()

	log.Debug("Closing file: ", w.File)
	return w.File.Close()
}
