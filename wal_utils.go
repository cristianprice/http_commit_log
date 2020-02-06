package main

import (
	"fmt"
	"hash/crc32"
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
func MoveToLastValidWalEntry(file os.File) (int64, error) {

}
