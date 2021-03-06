package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestMoveToLastValidWalEntryOk(t *testing.T) {

	tmpFile := fmt.Sprint(os.TempDir(), string(os.PathSeparator), "test_file.bin")
	szBytes := make([]byte, 4)

	var size uint32 = 40
	binary.LittleEndian.PutUint32(szBytes, size)

	t.Log("Populating file: ", tmpFile)

	file, err := os.Create(tmpFile)
	if err != nil {
		t.Error("Failed to open file: ", err)
		return
	}

	n, err := file.Write(szBytes)
	if n < binary.Size(size) || err != nil {
		t.Error("Failed to write to file: ", err)
		return
	}

	cnt := make([]byte, size)
	n, err = file.Write(cnt)
	if uint32(n) < size || err != nil {
		t.Error("Failed to write to file: ", err)
		return
	}

	file.Close()

	file, err = os.Open(tmpFile)
	if err != nil {
		t.Error("Failed to open file: ", err)
		return
	}

	defer file.Close()

	offset, err := MoveToLastValidWalEntry(file, 10000)
	if err != nil {
		t.Error("Failed to read file: ", err)
		return
	}

	if offset != 44 {
		t.Error("Should have returned 44 but found: ", offset)
		return
	}

	t.Log("Offset: ", offset)
}

func TestMoveToLastValidWalEntryNotOk(t *testing.T) {

	tmpFile := fmt.Sprint(os.TempDir(), string(os.PathSeparator), "test_file.bin")
	szBytes := make([]byte, 4)

	var size uint32 = 40
	binary.LittleEndian.PutUint32(szBytes, size)

	t.Log("Populating file: ", tmpFile)

	file, err := os.Create(tmpFile)
	if err != nil {
		t.Error("Failed to open file: ", err)
		return
	}

	n, err := file.Write(szBytes)
	if n < binary.Size(size) || err != nil {
		t.Error("Failed to write to file: ", err)
		return
	}

	cnt := make([]byte, size)
	n, err = file.Write(cnt)
	if uint32(n) < size || err != nil {
		t.Error("Failed to write to file: ", err)
		return
	}

	//Write jibberish
	szBytes[0] = 5
	szBytes[1] = 1

	n, err = file.Write(szBytes)
	if uint32(n) < 4 || err != nil {
		t.Error("Failed to write to file: ", err)
		return
	}

	n, err = file.Write(cnt)
	if n < binary.Size(cnt) || err != nil {
		t.Error("Failed to write to file: ", err)
		return
	}

	file.Close()

	file, err = os.Open(tmpFile)
	if err != nil {
		t.Error("Failed to open file: ", err)
		return
	}

	offset, err := MoveToLastValidWalEntry(file, 10000)
	if err != nil {
		t.Error("Failed to read file: ", err)
		return
	}

	if offset != 44 {
		t.Error("Should have returned 44 but found: ", offset)
	}

	t.Log("Offset: ", offset)
}
