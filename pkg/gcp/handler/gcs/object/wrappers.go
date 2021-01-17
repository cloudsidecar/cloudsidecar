package object

import (
	"io"
)

type CopyResponse struct {
	Kind string `json:"kind"`
	TotalBytesRewritten int64 `json:"totalBytesRewritten"`
	ObjectSize int64 `json:"objectSize"`
	Done bool `json:"done"`
	Resource string `json:"resource"`
}

type MultiObjectReader struct {
	Readers []io.ReadCloser
	readerIndex int
}

func (multiObjectReader *MultiObjectReader) Read(p []byte) (n int, err error) {
	currentReader := multiObjectReader.Readers[multiObjectReader.readerIndex]
	bytesRead, err := currentReader.Read(p)
	if err != nil && err == io.EOF {
		multiObjectReader.readerIndex += 1
		if multiObjectReader.readerIndex >= len(multiObjectReader.Readers) {
			return bytesRead, err
		} else {
			err = nil
		}
	}
	return bytesRead ,err
}

func (multiObjectReader *MultiObjectReader) Close() error {
	for _, reader := range multiObjectReader.Readers {
		if err := reader.Close(); err != nil {
			return err
		}
	}
	return nil
}
