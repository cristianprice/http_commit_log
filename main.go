package main

import (
	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting server on port: ", 8080)

	config := ReadConfig()

	a := ""
	config.HTTPServerConfig.Host = &a
}
