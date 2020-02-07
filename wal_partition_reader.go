package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

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
func NewWalPartitionReader(partitionParentDir string, partitionNumber uint32, walFile string, offset int64) (*WalPartitionReader, error) {
	partitionDir := fmt.Sprintf("%s%c%d", partitionParentDir, os.PathSeparator, partitionNumber)
	err := os.MkdirAll(partitionDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, reader, err := createReader(partitionParentDir, walFile, offset)
	if err != nil {
		return nil, err
	}

	ret := &WalPartitionReader{
		Closed:          false,
		PartitionDir:    partitionDir,
		File:            file,
		Reader:          bufio.NewReader(*reader),
		PartitionNumber: partitionNumber,
	}

	return ret, nil
}

func createReader(partitionParentDir string, walFile string, offset int64) (*os.File, *io.Reader, error) {
	return nil, nil, nil
}

func (w *WalPartitionReader) ReadNextEntry() (wr *WalExRecord, currentOffset int64, err error) {
	return nil, -1, nil
}

//Close closes the underlaying file handle.
func (w *WalPartitionReader) Close() {

	err := w.File.Close()
	if err != nil {
		log.Error("Failed to close file: ", w.File, " ", err)
	}
}
