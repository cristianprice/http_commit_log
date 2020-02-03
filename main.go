package main

import (
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	pw, err := NewWalPartitionWriter("c:\\tmp", 1, 10000, FlushOnCommit)
	if err != nil {
		panic(err)
	}

	wr := &WalRecord{
		Key:   "uuid1111",
		Value: []byte{0x00, 0x01, 0x02, 0x03},
	}

	b, err := wr.Bytes()
	cnt, err := wr.Write(b)
	println(cnt)

	wrEx := NewWalExRecord(wr, 1, time.Now().UnixNano())

	pw.Write(wrEx)
	defer pw.Close()
}
