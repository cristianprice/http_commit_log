package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

//Crc32 checksums a byte array.
func Crc32(b []byte) (uint32, error) {
	crc32q := crc32.MakeTable(crc32.Koopman)
	hash := crc32.New(crc32q)

	hash.Write(b)
	return hash.Sum32(), nil
}

//GenFileNameWith generates the path to wal file.
func GenFileNameWith(partitionDir string, fileName string) string {
	return fmt.Sprintf("%s%c%s", partitionDir, os.PathSeparator, fileName)
}

//GenFileName generates a new wal file based on nanoseconds.
func GenFileName(partitionDir string) string {
	return fmt.Sprintf("%s%c%d.wal", partitionDir, os.PathSeparator, time.Now().UnixNano())
}

//MoveToLastValidWalEntry will move the offset to the end of last valid entry
func MoveToLastValidWalEntry(file *os.File, sizeLimit int64) (int64, error) {
	var ret int64
	var retValid int64
	szBytes := []byte{0, 0, 0, 0}

	for {
		n, err := io.ReadFull(file, szBytes)
		if n == 0 && err == io.EOF {
			return retValid, nil
		} else if err == io.ErrUnexpectedEOF {
			return ret, err
		}

		ret += int64(n)

		sz := binary.LittleEndian.Uint32(szBytes)
		if int64(sz) > sizeLimit {
			return -1, fmt.Errorf("Content size if above %d", sizeLimit)
		}

		cnt := make([]byte, sz)
		n, err = io.ReadFull(file, cnt)
		if n == 0 && err == io.EOF {
			ret += int64(n)
			return ret, nil
		} else if err == io.ErrUnexpectedEOF {
			return retValid, nil
		}

		ret += int64(n)
		retValid = ret
	}
}
