package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Test struct {
	Title  string
	Reader io.Reader
}

func PersistSomeStuff() {
	f, err := os.Open("c:\\create_tables.sql")
	if err != nil {
		panic(err)
	}

	test := &Test{
		Title:  "Text To be Serialized",
		Reader: bufio.NewReader(f),
	}
	buf := new(bytes.Buffer)

	titleBytes := []byte(test.Title)
	err = binary.Write(buf, binary.LittleEndian, uint32(len(titleBytes)))
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	err = binary.Write(buf, binary.LittleEndian, titleBytes)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}

	fmt.Println("Content: ", buf)
}
