package main

import (
	"encoding/json"
	"flag"
	"os"

	"github.com/gdexlab/go-render/render"
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

	log.SetLevel(log.DebugLevel)
	log.SetReportCaller(true)
}

//WalSyncType sets the type of commit.
type WalSyncType string

const (

	//FlushOnCommit flushes to disk after each write. Slow performance.
	FlushOnCommit WalSyncType = "SyncOnTxEnd"

	//WaitForBatchOrTimeout Waits for a set number of commits or a timeout.
	WaitForBatchOrTimeout WalSyncType = "WaitForBatchOrTimeout"
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

//ReadConfig reads config from a file.
func ReadConfig() *Config {
	ret := &Config{}
	return ret.readFile()
}

func (c *Config) readFile() *Config {
	configFilePath := flag.String("config", "config.json", "Provide a config file.")
	flag.Parse()

	f, err := os.Open(*configFilePath)
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

	log.Debug("Config values: ", render.Render(c))

	return c
}
