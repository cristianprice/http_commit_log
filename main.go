package main

import (
	"runtime"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	runtime.GOMAXPROCS(1)

	twr, err := NewTopicWriter("c:\\tmp", "Coco", 2, 20, FlushOnCommit)
	if err != nil {
		panic(err)
	}

	retChan := twr.WriteWalRecord(&WalRecord{
		Key:   "Hey",
		Value: []byte{11, 223, 3},
	})

	log.Info(<-retChan)

	retChan = twr.WriteWalRecord(&WalRecord{
		Key:   "Hey1",
		Value: []byte{11, 223, 3},
	})

	log.Info(<-retChan)

	retChan = twr.WriteWalRecord(&WalRecord{
		Key:   "Hey2",
		Value: []byte{11, 223, 3},
	})

	log.Info(<-retChan)

	WaitForCtrlC()

	//time.Sleep(15 * time.Second)
	defer twr.Close()
}
