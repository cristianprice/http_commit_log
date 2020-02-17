package main

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func path() string {
	return Path(os.TempDir()).AddInt64(time.Now().UnixNano() + rand.Int63()).AddExtension(".wal").String()
}

func BenchmarkWriteSpeedNoSync(b *testing.B) {
	b.ResetTimer()

	tmpFile := path()
	writer, err := NewWalPartitionWriter(tmpFile, 10000000000000, WaitForBatchOrTimeout)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		b := make([]byte, rand.Int31()%10000)
		_, err := writer.Write(b)
		if err == ErrSegLimitReached {
			log.Fatal(err)
		} else if err != nil {
			log.Fatal(err)
		}
	}

	writer.Close()
}

func BenchmarkWriteSpeedSync(b *testing.B) {
	b.ResetTimer()

	tmpFile := path()
	writer, err := NewWalPartitionWriter(tmpFile, 10000000000000, FlushOnCommit)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		b := make([]byte, rand.Int31()%10000)
		_, err := writer.Write(b)
		if err == ErrSegLimitReached {
			log.Fatal(err)
		} else if err != nil {
			log.Fatal(err)
		}

		err = writer.Flush()
		if err != nil {
			log.Fatal(err)
		}
	}

	writer.Close()
}