package main

import (
	"flag"
	"os"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
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
		Port *int    `yaml:"port"`
		Host *string `yaml:"host"`
	} `yaml:"httpServer"`

	LogFile struct {
		DefaultLogBehaviour   *string `yaml: "defaultLogBehaviour"`
		MaxFileSize           *int    `yaml: "maxFileSize"`
		MaxLogEntrySize       *int    `yaml: "maxLogEntrySize"`
		LogFlushTimeoutMillis *int64  `yaml: "logFlushTimeoutMillis"`
	} `yaml:"logFile"`
}

func ReadConfig() *Config {
	ret := &Config{}
	return ret.readFile()
}

func (c *Config) readCmdLine() *Config {
	c.HTTPServerConfig.Host = flag.String("host", "localhost", "Host to bind.")

	flag.Parse()
	return c
}

func (c *Config) readFile() *Config {
	f, err := os.Open("config.yml")
	if err != nil {
		log.Warn("Could not find config file, setting defaults. ", err)
		return c
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(c)
	if err != nil {
		log.Warn("Could not find config file, setting defaults. ", err)
	}

	return c
}
