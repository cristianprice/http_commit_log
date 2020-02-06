package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	log "github.com/sirupsen/logrus"
)

//WalPartitionReader abstraction for individual partition writer.
type WalPartitionReader struct {
	Closed          bool
	PartitionDir    string
	PartitionNumber uint32

	File   *os.File
	Reader *bufio.Reader

	CurrentOffset int64
}

//NewWalPartitionReader creates a new WalPartitionReader
func NewWalPartitionReader(partitionParentDir string, partitionNumber uint32) (*WalPartitionReader, error) {
	partitionDir := fmt.Sprintf("%s%c%d", partitionParentDir, os.PathSeparator, partitionNumber)
	err := os.MkdirAll(partitionDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, writer, err := createWriter(partitionDir, maxSegmentSize)
	if err != nil {
		return nil, err
	}

	ret := &WalPartitionReader{
		Closed:          false,
		PartitionDir:    partitionDir,
		File:            file,
		Writer:          writer,
		MaxSegmentSize:  maxSegmentSize,
		PartitionNumber: partitionNumber,
		WalSyncType:     wst,
	}

	return ret, nil
}

func (w *WalPartitionWriter) Read(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

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
func (w *WalPartitionWriter) Flush() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	err := w.Writer.Flush()
	if err != nil {
		log.Warn("Failed to flush to file: ", w.File, " ", err)
	}

	if w.WalSyncType == FlushOnCommit {
		err = w.File.Sync()
		if err != nil {
			log.Error("Failed to sync file: ", w.File, " ", err)
		}
	}
}

//Close closes the underlaying file handle.
func (w *WalPartitionWriter) Close() {
	w.Flush()

	err := w.File.Close()
	if err != nil {
		log.Error("Failed to close file: ", w.File, " ", err)
	}
}

func returnLastCreatedWalFile(partitionDir string) (*string, error) {
	files, err := ioutil.ReadDir(partitionDir)
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i1, i2 int) bool {
		return files[i1].Name() < files[i2].Name()
	})

	if len(files) == 0 {
		ret := ""
		return &ret, nil
	}

	ret := genFileNameWith(partitionDir, files[0].Name())
	return &ret, nil
}
