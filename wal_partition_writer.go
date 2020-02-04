package main

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type WalPartitionWriter struct {
	mutex sync.Mutex

	Closed          bool
	PartitionDir    string
	PartitionNumber uint32
	MaxSegmentSize  int64

	WalSyncType WalSyncType
	File        *os.File
	Writer      *bufio.Writer

	CurrentOffset int64
}

func Crc32(b []byte) (uint32, error) {
	crc32q := crc32.MakeTable(crc32.Koopman)
	hash := crc32.New(crc32q)

	hash.Write(b)
	return hash.Sum32(), nil
}

func NewWalPartitionWriter(partitionParentDir string, partitionNumber uint32, maxSegmentSize int64, wst WalSyncType) (*WalPartitionWriter, error) {
	partitionDir := fmt.Sprintf("%s%c%d", partitionParentDir, os.PathSeparator, partitionNumber)
	err := os.MkdirAll(partitionDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, writer, err := createWriter(partitionDir, maxSegmentSize)
	if err != nil {
		return nil, err
	}

	ret := &WalPartitionWriter{
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

func (w *WalPartitionWriter) Write(wr *WalExRecord) int {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	count, err := w.Writer.Write(nil)
	if err != nil {
		log.Panic("Failed to write byte count: ", len(wr.Record.Value))
	}

	return count
}

func (w *WalPartitionWriter) Close() {
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

	w.File.Close()
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

func genFileNameWith(partitionDir string, fileName string) string {
	return fmt.Sprintf("%s%c%s", partitionDir, os.PathSeparator, fileName)
}

func genFileName(partitionDir string) string {
	return fmt.Sprintf("%s%c%d.wal", partitionDir, os.PathSeparator, time.Now().UnixNano())
}

func createWriter(partitionDir string, maxSegmentSize int64) (*os.File, *bufio.Writer, error) {
	var file *os.File
	fileName, err := returnLastCreatedWalFile(partitionDir)
	if err != nil || *fileName == "" {
		*fileName = genFileName(partitionDir)
	}

	fi, err := os.Stat(*fileName)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	} else if os.IsNotExist(err) {
		log.Warn("File: ", *fileName, " does not exist. Creating ...")
	} else if fi.Size() >= (maxSegmentSize - maxSegmentSize*8/10) {
		oldFileName := fileName
		*fileName = genFileName(partitionDir)
		log.Warn("File: ", *oldFileName, " exceeds size. Creating new one: ", *fileName)
	}

	file, err = os.OpenFile(*fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}

	writer := bufio.NewWriter(file)
	return file, writer, nil
}
