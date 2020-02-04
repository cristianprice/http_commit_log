package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

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

func (wr *WalExRecord) Bytes() ([]byte, error) {
	buff := bytes.Buffer{}
	tmpBuff := make([]byte, 8)

	binary.LittleEndian.PutUint64(tmpBuff, uint64(wr.Id.Timestamp))
	buff.Write(tmpBuff)

	tmpBuff = tmpBuff[:4]
	binary.LittleEndian.PutUint32(tmpBuff, uint32(wr.Id.Sequence))
	buff.Write(tmpBuff)

	recBuff, err := wr.Record.Bytes()
	if err != nil {
		return nil, err
	}

	buff.Write(recBuff)

	tmpBuff = tmpBuff[:4]
	binary.LittleEndian.PutUint32(tmpBuff, uint32(wr.Crc))
	buff.Write(tmpBuff)

	return buff.Bytes(), nil
}

func (wer *WalExRecord) Write(p []byte) (n int, err error) {
	var idx uint32 = 0
	var tmpUint64 uint64 = 0

	if len(p) < binary.Size(tmpUint64) {
		return -1, errors.New("Slice length not large enough. Could not read timestamp.")
	}

	wer.Id.Timestamp = int64(binary.LittleEndian.Uint64(p))
	idx += uint32(binary.Size(wer.Id.Timestamp))

	if len(p[idx:]) < binary.Size(wer.Id.Sequence) {
		return -1, errors.New("Slice length not large enough. Could not read sequence.")
	}

	wer.Id.Sequence = uint32(binary.LittleEndian.Uint32(p[idx:]))
	idx += uint32(binary.Size(wer.Id.Sequence))

	cnt, err := wer.Record.Write(p[idx:])
	if err != nil {
		return -1, err
	}

	idx += uint32(cnt)
	if len(p[idx:]) < binary.Size(wer.Crc) {
		return -1, errors.New("Slice length not large enough. Could not read Crc.")
	}

	wer.Crc = uint32(binary.LittleEndian.Uint32(p[idx:]))
	return int(idx) + binary.Size(wer.Crc), nil
}

func (wr *WalExRecord) Read(p []byte) (n int, err error) {

	b, err := wr.Bytes()
	if err != nil {
		return -1, err
	}

	return copy(p, b), nil
}
