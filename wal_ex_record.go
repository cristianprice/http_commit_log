package main

import (
	"bytes"
	"encoding/binary"
)

//WalRecordID a complex identification for wal entries.
type WalRecordID struct {
	Timestamp int64
	Sequence  uint32
	Partition int32
}

//WalExRecord extended wal record, includes the id and the crc.
type WalExRecord struct {
	Record *WalRecord
	ID     *WalRecordID
	Crc    uint32
}

//NewWalExRecord creates a new extended wal record from key and value.
func NewWalExRecord(wr *WalRecord, sequence uint32, timestamp int64) *WalExRecord {

	ret := &WalExRecord{
		Record: wr,
		ID: &WalRecordID{
			Timestamp: timestamp,
			Sequence:  sequence,
		},
	}

	b, err := ret.Bytes()
	if err != nil {
		panic(err)
	}

	ret.Crc, err = Crc32(b[:(len(b) - binary.Size(ret.Crc))])
	if err != nil {
		panic(err)
	}

	return ret
}

//Bytes returns the byte representation of this structure. Partition is not included.
func (wr *WalExRecord) Bytes() ([]byte, error) {
	buff := bytes.Buffer{}
	tmpBuff := make([]byte, 8)

	binary.LittleEndian.PutUint64(tmpBuff, uint64(wr.ID.Timestamp))
	buff.Write(tmpBuff)

	tmpBuff = tmpBuff[:4]
	binary.LittleEndian.PutUint32(tmpBuff, uint32(wr.ID.Sequence))
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

//Write implements the actual io.Writer interface. Fails if the exact number of bytes is not provided.
func (wr *WalExRecord) Write(p []byte) (n int, err error) {
	var idx uint32 = 0
	var tmpUint64 uint64 = 0

	if len(p) < binary.Size(tmpUint64) {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read timestamp.")
	}

	wr.ID.Timestamp = int64(binary.LittleEndian.Uint64(p))
	idx += uint32(binary.Size(wr.ID.Timestamp))

	if len(p[idx:]) < binary.Size(wr.ID.Sequence) {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read sequence.")
	}

	wr.ID.Sequence = uint32(binary.LittleEndian.Uint32(p[idx:]))
	idx += uint32(binary.Size(wr.ID.Sequence))

	cnt, err := wr.Record.Write(p[idx:])
	if err != nil {
		return -1, err
	}

	idx += uint32(cnt)
	if len(p[idx:]) < binary.Size(wr.Crc) {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read Crc.")
	}

	wr.Crc = uint32(binary.LittleEndian.Uint32(p[idx:]))
	return int(idx) + binary.Size(wr.Crc), nil
}

func (wr *WalExRecord) Read(p []byte) (n int, err error) {

	b, err := wr.Bytes()
	if err != nil {
		return -1, err
	}

	return copy(p, b), nil
}
