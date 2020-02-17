package main

import (
	"os"
	"os/signal"
	"sync"

	log "github.com/sirupsen/logrus"
)

//WaitForCtrlC implementation to wait for a signal.
func WaitForCtrlC() {
	var endWaiter sync.WaitGroup
	endWaiter.Add(1)
	var signalChannel chan os.Signal
	signalChannel = make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	go func() {
		<-signalChannel
		endWaiter.Done()
	}()

	log.Info("Waiting for CTRL+C ...")
	endWaiter.Wait()
}
