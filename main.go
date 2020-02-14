package main

import (
	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	twr, err := NewTopicWriter("c:\\tmp", "Coco", 2, 10000, FlushOnCommit)
	if err != nil {
		panic(err)
	}

	retChan := twr.WriteWalRecord(&WalRecord{
		Key:   "Hey",
		Value: []byte{11, 223, 3},
	})

	print(<-retChan)

	retChan = twr.WriteWalRecord(&WalRecord{
		Key:   "Hey1",
		Value: []byte{11, 223, 3},
	})

	print(<-retChan)

	retChan = twr.WriteWalRecord(&WalRecord{
		Key:   "Hey2",
		Value: []byte{11, 223, 3},
	})

	print(<-retChan)

	WaitForCtrlC()

	//time.Sleep(15 * time.Second)
	defer twr.Close()
}
