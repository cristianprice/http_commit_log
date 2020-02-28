package main

import (
	"encoding/json"
	"io"
)

//WalSyncType sets the type of commit.
type WalSyncType string

const (

	//FlushOnCommit flushes to disk after each write with a sync. Slow performance.
	FlushOnCommit WalSyncType = "SyncOnTxEnd"

	//NoFlush typical flush on handle only. No guarantees for disk persistence.
	NoFlush WalSyncType = "WaitForBatchOrTimeout"
)

//WalTopicConfig serializes the topic config to file.
type WalTopicConfig struct {
	Name           string       `json:"name"`
	PartitionCount uint32       `json:"partitionCount"`
	WalSyncType    *WalSyncType `json:"walSyncType"`
}

//WalTopicsConfig a collection of topic config.
type WalTopicsConfig []WalTopicConfig

//ReadConfig from reader.
func (wc *WalTopicsConfig) ReadConfig(r *io.Reader) error {
	decoder := json.NewDecoder(*r)
	return decoder.Decode(*wc)
}

//WriteConfig to writer.
func (wc *WalTopicsConfig) WriteConfig(wr *io.Writer) error {
	decoder := json.NewEncoder(*wr)
	return decoder.Encode(*wc)
}
