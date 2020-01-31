package main

import (
	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	pw, err := NewWalPartitionWriter("c:\\tmp", 1, 10000, FlushOnCommit)
	if err != nil {
		panic(err)
	}

	b := []byte{0x00, 0x01}
	pw.Write(b)
	defer pw.Close()
}
