package main

import (
	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	w := NewWalWriter("sd", 1, "Wee")
	println(w)
}
