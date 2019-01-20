package object

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

type ChunkedReaderWrapper struct {
	Reader            *io.ReadCloser
	ContentLength     *int64
	Buffer            []byte
	ChunkNextPosition int
	ChunkSize         int
}

func (wrapper *ChunkedReaderWrapper) ReadHeaderGetChunkSize() (i int, err error) {
	chunkedHeader, err := wrapper.ReadHeader()
	if err != nil {
		fmt.Printf("Error reading header %s", err)
		return 0, err
	}
	fmt.Printf("Read header %s\n", chunkedHeader)
	chunkedSplit := strings.SplitN(chunkedHeader, ";", 2)
	chunkSize, err := strconv.ParseInt(chunkedSplit[0], 16, 32)
	return int(chunkSize), err
}

func (wrapper *ChunkedReaderWrapper) ReadHeader() (s string, err error) {
	oneByte := make([]byte, 1)
	readCount := 0
	header := make([]byte, 4096)
	for {
		_, err := io.ReadFull(*wrapper.Reader, oneByte)
		if err != nil {
			return string(header[:readCount]), err
		}
		if oneByte[0] == '\r' {
			// read \n
			io.ReadFull(*wrapper.Reader, oneByte)
			if readCount != 0 {
				return string(header[:readCount]), nil
			} else {
				// \r is first char
				io.ReadFull(*wrapper.Reader, oneByte)
			}
		}
		if readCount >= len(header) {
			return string(header[:readCount]), io.ErrShortBuffer
		}
		header[readCount] = oneByte[0]
		readCount++
	}
}

func (wrapper *ChunkedReaderWrapper) Read(p []byte) (n int, err error) {
	if wrapper.Buffer == nil || len(wrapper.Buffer) == 0 {
		wrapper.ChunkNextPosition = 0
		chunkSize, err := wrapper.ReadHeaderGetChunkSize()
		fmt.Printf("Chunk size %d\n", chunkSize)
		if err != nil {
			fmt.Printf("Error reading header %s", err)
			return 0, err
		}
		wrapper.ChunkSize = chunkSize
		if chunkSize == 0 {
			return 0, io.EOF
		}
		buffer := make([]byte, chunkSize)
		wrapper.Buffer = buffer
		_, err = io.ReadFull(*wrapper.Reader, buffer)
		if err != nil {
			fmt.Printf("Error reading all %s", err)
			return 0, err
		}
	}
	// 0: wrapper.Buffer = 5, CNP = 0, bytesLeft = 5
	// pSize = 2
	// read [0, 2]
	// 1: CNP = 2, bytesLeft = 3
	// read [2, 4]
	// 2: CNP = 4, bytesLeft = 1
	bytesLeft := len(wrapper.Buffer) - wrapper.ChunkNextPosition
	pSize := len(p)
	if pSize <= bytesLeft {
		nextPos := wrapper.ChunkNextPosition + pSize
		copy(p, (wrapper.Buffer)[wrapper.ChunkNextPosition:nextPos])
		wrapper.ChunkNextPosition = nextPos
		fmt.Println("READO ", pSize, bytesLeft, len(wrapper.Buffer))
		return pSize, nil
	} else {
		fmt.Println("DONE READO ", pSize, bytesLeft, len(wrapper.Buffer))
		n := copy(p, wrapper.Buffer[wrapper.ChunkNextPosition:])
		wrapper.Buffer = nil
		return n, nil
	}
}
