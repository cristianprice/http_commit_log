package main

import (
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
)

type Color string

const (
	ColorBlack  Color = "\u001b[30m"
	ColorRed          = "\u001b[31m"
	ColorGreen        = "\u001b[32m"
	ColorYellow       = "\u001b[33m"
	ColorBlue         = "\u001b[34m"
	ColorReset        = "\u001b[0m"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})
}

const (
	FlushOnCommit         = "SyncOnTxEnd"
	WaitForBatchOrTimeout = "WaitForBatchOrTimeout"
)

//Config is a configuration struct.
type Config struct {
	HTTPServerConfig struct {
		Port *int    `json:"port"`
		Host *string `json:"host"`
	} `json:"server"`
	LogFile struct {
		DefaultLogBehaviour   *string `json:"defaultLogBehaviour"`
		MaxLogFileSize        *int    `json:"maxLogFileSize"`
		MaxLogEntrySize       *int    `json:"maxLogEntrySize"`
		LogFlushTimeoutMillis *int    `json:"logFlushTimeoutMillis"`
	} `json:"logFile"`
}

func ReadConfig() *Config {
	ret := &Config{}
	return ret.readFile()
}

func (c *Config) readFile() *Config {
	f, err := os.Open("config.yml")
	if err != nil {
		log.Warn("Could not find config file, setting defaults. ", err)
		return c
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	err = decoder.Decode(c)
	if err != nil {
		log.Warn("Could not find config file, setting defaults. ", err)
	}

	return c
}
