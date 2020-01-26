package main

import (
	envconfig "github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
	"os"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})
}

const (
	FlushOnCommit = 0,
	WaitForTimeout=1,
)

//Config is a configuration struct.
type Config struct {
	HTTPServerConfig struct {
		Port *int `yaml:"port"`
		Host *string `yaml:"host"`
	} `yaml:"httpServer"`

	LogFile struct {
		MaxFileSize *int `yaml: "maxFileSize"`
		MaxLogEntrySize *int  `yaml: "maxLogEntrySize"`
		LogFlushTimeoutMillis *int64 `yaml: "logFlushTimeoutMillis"`
	}`yaml:"logFile"`
}



func (c *Config) ReadConfig() *Config {
	return c.readFile()
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
