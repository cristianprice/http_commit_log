package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

//Path is a directory path abstraction.
type Path struct {
	CurrentPath string
}

//AddExtension is a builder method.
func (p *Path) AddExtension(ext string) *Path {
	ret := &Path{
		CurrentPath: fmt.Sprint(p.CurrentPath, ext),
	}
	return ret
}

//AddUint32 is a builder method.
func (p *Path) AddUint32(child uint32) *Path {
	return p.Add(fmt.Sprint(child))
}

//AddInt64 is a builder method.
func (p *Path) AddInt64(child int64) *Path {
	return p.Add(fmt.Sprint(child))
}

//Add is a builder method.
func (p *Path) Add(child string) *Path {
	ret := &Path{
		CurrentPath: fmt.Sprint(p.CurrentPath, string(os.PathSeparator), child),
	}
	return ret
}

//Crc32 checksums a byte array.
func Crc32(b []byte) (uint32, error) {
	crc32q := crc32.MakeTable(crc32.Koopman)
	hash := crc32.New(crc32q)

	hash.Write(b)
	return hash.Sum32(), nil
}

//GenFileNameWith generates the path to wal file.
func GenFileNameWith(partitionDir string, fileName string) string {
	return fmt.Sprint(partitionDir, string(os.PathSeparator), fileName)
}

//GenFileName generates a new wal file based on nanoseconds.
func GenFileName(partitionDir string) string {
	return fmt.Sprint(partitionDir, string(os.PathSeparator), time.Now().UnixNano(), ".wal")
}

//ReturnLastCreatedWalFile returns the last created wal segment file.
func ReturnLastCreatedWalFile(partitionDir string) (*string, error) {
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

	ret := GenFileNameWith(partitionDir, files[0].Name())
	return &ret, nil
}

func CreateTempWriter(partitionDir string, maxSegmentSize int64) (*os.File, *bufio.Writer, int64, error) {
	var file *os.File
	fileName, err := ReturnLastCreatedWalFile(partitionDir)
	if err != nil || *fileName == "" {
		*fileName = GenFileName(partitionDir)
	}

	file, err = os.OpenFile(*fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, -1, err
	}

	offset, err := MoveToLastValidWalEntry(file, maxSegmentSize)
	if err != nil {
		return nil, nil, -1, err
	}

	if offset > maxSegmentSize {
		oldFileName := fileName
		*fileName = GenFileName(partitionDir)
		log.Warn("File: ", *oldFileName, " exceeds size. Creating new one: ", *fileName)
		file.Close()

		file, err = os.OpenFile(*fileName, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, nil, -1, err
		}

		offset = 0
	}

	offset, err = file.Seek(offset, 0)
	writer := bufio.NewWriter(file)

	return file, writer, offset, nil
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
