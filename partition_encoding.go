package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

type WalRecord struct {
	Key   string
	Value []byte
}

type WalRecordId struct {
	Timestamp int64
	Sequence  uint32
	Partition int32
}

type WalExRecord struct {
	Record *WalRecord
	Id     *WalRecordId
	Crc    int32
}

type WalPartitionWriter struct {
	PartitionDir    string
	PartitionNumber uint32

	Writer *io.Writer
}

func NewWalPartitionWriter(partitionParentDir string, partitionNumber uint32) (*WalPartitionWriter, error) {
	partitionDir := fmt.Sprintf("%s%c%d", partitionParentDir, os.PathSeparator, partitionNumber)
	err := os.MkdirAll(partitionDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	writer, err := createWriter(partitionDir)
	if err != nil {
		return nil, err
	}

	ret := &WalPartitionWriter{
		PartitionDir: partitionDir,
		Writer:       writer,
	}

	return ret, nil
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

	ret := files[0].Name()
	return &ret, nil
}

func createWriter(partitionDir string) (*io.Writer, error) {

	return nil, nil
}
