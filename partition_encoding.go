package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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
	Crc    uint32
}

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

func (wr *WalRecord) Write(p []byte) (n int, err error) {
	var idx uint32 = 0

	if len(p) < 4 {
		return -1, errors.New("Slice length not large enough. Could not read key length.")
	}

	length := binary.LittleEndian.Uint32(p[idx:])
	idx += uint32(binary.Size(length))

	if uint32(len(p[idx:])) < length {
		return -1, errors.New("Slice length not large enough. Could not read key.")
	}

	wr.Key = string(p[idx:(idx + length)])
	idx += length

	if uint32(len(p[idx:])) < 4 {
		return -1, errors.New("Slice length not large enough. Could not read value length.")
	}

	length = binary.LittleEndian.Uint32(p[idx:])
	idx += uint32(binary.Size(length))

	if uint32(len(p[idx:])) < length {
		return -1, errors.New("Slice length not large enough. Could not read value.")
	}

	wr.Value = p[idx:]

	return int(idx + length), nil
}

func (wr *WalRecord) Read(p []byte) (n int, err error) {

	b, err := wr.Bytes()
	if err != nil {
		return -1, err
	}

	return copy(p, b), nil
}

func (wr *WalRecord) Bytes() ([]byte, error) {
	buff := bytes.Buffer{}
	tmpBuff := make([]byte, 4)

	//Encode key Len and key first.
	valBytes := []byte(wr.Key)
	binary.LittleEndian.PutUint32(tmpBuff, uint32(len(valBytes)))

	_, err := buff.Write(tmpBuff)
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(valBytes)
	if err != nil {
		return nil, err
	}

	//Now encode value len and value.
	valBytes = []byte(wr.Value)
	binary.LittleEndian.PutUint32(tmpBuff, uint32(len(valBytes)))

	_, err = buff.Write(tmpBuff)
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(valBytes)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func writeTo(w io.Writer, timestamp int64, sequence uint32) (n int, err error) {
	b := make([]byte, binary.Size(timestamp)+binary.Size(sequence))
	binary.LittleEndian.PutUint64(b, uint64(timestamp))
	binary.LittleEndian.PutUint32(b[8:], uint32(sequence))

	return w.Write(b)
}

func NewWalExRecord(wr *WalRecord, sequence uint32, timestamp int64) *WalExRecord {

	ret := &WalExRecord{
		Record: wr,
		Id: &WalRecordId{
			Timestamp: timestamp,
			Sequence:  sequence,
		},
	}

	crc32q := crc32.MakeTable(crc32.Koopman)
	hash := crc32.New(crc32q)

	writer := io.Writer(hash)
	c, err := writeTo(writer, timestamp, sequence)
	if err != nil || c < binary.Size(sequence)+binary.Size(timestamp) {
		log.Panic("Failed to write bytes ...: ", err)
		return nil
	}

	c, err = hash.Write([]byte(ret.Record.Key))
	if err != nil || c < len(ret.Record.Key) {
		log.Panic("Failed to write bytes ...: ", err)
		return nil
	}

	c, err = hash.Write(ret.Record.Value)
	if err != nil || c < len(ret.Record.Value) {
		log.Panic("Failed to write bytes ...: ", err)
		return nil
	}

	ret.Crc = hash.Sum32()

	return ret
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
