package object

import (
	"errors"
	"io"
	"cloudsidecar/pkg/logging"
	"strconv"
	"strings"
)

type ReaderWrapper struct {
	Reader io.ReadCloser
}

func (wrapper ReaderWrapper) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("invalid operation")
}

func (wrapper ReaderWrapper) Read(p []byte) (n int, err error) {
	return (wrapper.Reader).Read(p)
}

type ChunkedReaderWrapper struct {
	Reader            *io.ReadCloser
	ChunkNextPosition int
	ChunkSize         int
}

func (wrapper *ChunkedReaderWrapper) ReadHeaderGetChunkSize() (i int, err error) {
	chunkedHeader, err := wrapper.ReadHeader()
	if err != nil {
		logging.Log.Error("Error reading header %s", err)
		return 0, err
	}
	logging.Log.Info("Read header %s\n", chunkedHeader)
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
	if wrapper.ChunkNextPosition == -1 {
		wrapper.ChunkNextPosition = 0
		chunkSize, err := wrapper.ReadHeaderGetChunkSize()
		logging.Log.Debug("Chunk size %d\n", chunkSize)
		if err != nil {
			logging.Log.Error("Error reading header %s", err)
			return 0, err
		}
		wrapper.ChunkSize = chunkSize
		if chunkSize == 0 {
			return 0, io.EOF
		}
	}
	bytesLeft := wrapper.ChunkSize - wrapper.ChunkNextPosition
	pLen := len(p)
	if bytesLeft <= pLen {
		n, err = io.ReadFull(*wrapper.Reader, p[:bytesLeft])
		wrapper.ChunkNextPosition = -1
	} else {
		n, err = io.ReadFull(*wrapper.Reader, p)
		wrapper.ChunkNextPosition += n
	}
	return n, err
}
