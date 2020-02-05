package main

import (
	"encoding/binary"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	b := make([]byte, 8)
	b1 := b[4:]
	fmt.Println(binary.Size(b), binary.Size(b1))

	pw, err := NewWalPartitionWriter("c:\\tmp", 1, 10000, FlushOnCommit)
	if err != nil {
		panic(err)
	}

	wr := &WalRecord{
		Key:   "uuid1111",
		Value: []byte{0x00, 0x01, 0x02, 0x03},
	}

	wrEx := NewWalExRecord(wr, 1, time.Now().UnixNano())
	b, err = wrEx.Bytes()
	if err != nil {
		panic(err)
	}

	pw.Write(b)
	defer pw.Close()
}
