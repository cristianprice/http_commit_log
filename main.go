package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	twr, err := NewTopicWriter("c:\\tmp", "Coco", 2, 10000, FlushOnCommit)
	if err != nil {
		panic(err)
	}

	print(twr.Name)
	fmt.Println(twr, err)

	WaitForCtrlC()
}
