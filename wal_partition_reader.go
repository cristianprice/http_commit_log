package main

import (
	"bufio"
	"encoding/binary"
	"errors"
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
func NewWalPartitionReader(partitionParentDir string, partitionNumber uint32, walFile string) (*WalPartitionReader, error) {
	partitionDir := Path(partitionParentDir).AddUint32(partitionNumber)
	err := os.MkdirAll(partitionDir.String(), os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, err := createReader(partitionDir.Add(walFile).String())
	if err != nil {
		return nil, err
	}

	ret := &WalPartitionReader{
		Closed:          false,
		PartitionDir:    partitionDir.String(),
		File:            file,
		Reader:          bufio.NewReader(file),
		PartitionNumber: partitionNumber,
	}

	return ret, nil
}

func createReader(walFile string) (*os.File, error) {
	file, err := os.Open(walFile)
	if err != nil {
		return nil, err
	}

	return file, nil
}

//ReadNextEntry reads the next entry from this wal segment.
func (w *WalPartitionReader) ReadNextEntry() (*WalExRecord, int64, error) {
	size := []byte{0, 0, 0, 0}
	n, err := io.ReadFull(w.Reader, size)
	if err == io.ErrUnexpectedEOF {
		//Is this corrupted ?
		w.CurrentOffset += int64(n)
		return nil, w.CurrentOffset, err
	} else if err == io.EOF {
		return nil, w.CurrentOffset, err
	}

	//Succeeded in reading size.
	w.CurrentOffset += int64(n)
	sz := binary.LittleEndian.Uint32(size)
	buff := make([]byte, sz)
	n, err = io.ReadFull(w.Reader, buff)
	if err == io.ErrUnexpectedEOF {
		//Is this corrupted ?
		w.CurrentOffset += int64(n)
		return nil, w.CurrentOffset, err
	} else if err == io.EOF {
		return nil, w.CurrentOffset, err
	}

	//We succeeded reading content.
	w.CurrentOffset += int64(n)
	wr := &WalExRecord{
		Record: &WalRecord{},
		ID:     &WalRecordID{},
	}
	_, err = wr.Write(buff)
	if err != nil {
		//Unlikely to happen
		return nil, w.CurrentOffset, nil
	}

	//Check for Crc32.
	crc, err := Crc32(buff[:(sz - uint32(binary.Size(size)))])
	if crc != wr.Crc {
		return wr, w.CurrentOffset, errors.New("Wrong Checksum")
	}

	return wr, w.CurrentOffset, nil
}

//Close closes the underlaying file handle.
func (w *WalPartitionReader) Close() {

	err := w.File.Close()
	if err != nil {
		log.Error("Failed to close file: ", w.File, " ", err)
	}
}
