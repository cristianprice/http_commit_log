package main

import "io"

type LogEntry struct {
	key   string
	value io.Reader
}

type LogManager struct {
	PartitionCount      int
	PartitionName       string
	DefaultLogBehaviour string

	writerChannel chan LogEntry
}
