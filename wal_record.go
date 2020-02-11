package main

import (
	"bytes"
	"encoding/binary"
)

//WalRecord basic key value construct.
type WalRecord struct {
	Key   string
	Value []byte
}

func (wr *WalRecord) Read(p []byte) (n int, err error) {

	b, err := wr.Bytes()
	if err != nil {
		return -1, err
	}

	return copy(p, b), nil
}

func (wr *WalRecord) Write(p []byte) (n int, err error) {
	var idx uint32 = 0
	var length uint32 = 0

	if len(p) < binary.Size(length) {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read key length.")
	}

	length = binary.LittleEndian.Uint32(p[idx:])
	idx += uint32(binary.Size(length))

	if uint32(len(p[idx:])) < length {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read key.")
	}

	wr.Key = string(p[idx:(idx + length)])
	idx += length

	if uint32(len(p[idx:])) < 4 {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read value length.")
	}

	length = binary.LittleEndian.Uint32(p[idx:])
	idx += uint32(binary.Size(length))

	if uint32(len(p[idx:])) < length {
		return -1, NewWalError(ErrSliceNotLargeEnough, "Slice length not large enough. Could not read value.")
	}

	wr.Value = p[idx:(idx + length)]

	return int(idx + length), nil
}

//Bytes turns structure to bytes.
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
