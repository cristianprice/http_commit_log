package main

import (
	"fmt"
	"os"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
)

func pathTopic() Path {
	return Path(os.TempDir())
}

func BenchmarkTopicWriteSpeedNoSync(b *testing.B) {
	b.ResetTimer()

	tmpFile := pathTopic()
	writer, err := NewTopicWriter(tmpFile, "Test", 4, 256, WaitForBatchOrTimeout)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(b.N)

	for i := 0; i < b.N; i++ {

		wr := WalRecord{
			Key:   fmt.Sprint(i),
			Value: []byte("asdasdasdda"),
		}

		errChan := writer.WriteWalRecord(&wr)

		go func() {
			<-errChan
			wg.Done()
		}()
	}

	wg.Wait()
	writer.Close()
}
