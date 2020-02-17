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

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	offset, err := MoveToLastValidWalEntry(file, maxSegmentSize)
	if err != nil {
		return nil, err
	}

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
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.CurrentOffset > w.MaxSegmentSize {
		return -1, ErrSegLimitReached
	}

	size := []byte{0, 0, 0, 0}
	binary.LittleEndian.PutUint32(size, uint32(binary.Size(p)))

	ret := 0

	count, err := w.Writer.Write(size)
	if err != nil {
		return -1, err
	}

	ret += count

	count, err = w.Writer.Write(p)
	if err != nil {
		return -1, err
	}

	ret += count
	w.CurrentOffset += int64(ret)
	return ret, nil
}

//Flush flushes data to file handle based on options
func (w *WalPartitionWriter) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	err := w.Writer.Flush()
	if err != nil {
		log.Warn("Failed to flush to file: ", w.File, " ", err)
	}

	if w.WalSyncType == FlushOnCommit {
		err = w.File.Sync()
	}

	return err
}

//Close closes the underlaying file handle.
func (w *WalPartitionWriter) Close() error {
	w.Flush()

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.File.Close()
}
