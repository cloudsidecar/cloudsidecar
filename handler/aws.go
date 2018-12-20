package handler

import (
	"fmt"
	"net/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Handler struct {
	Client *s3.S3
}

func (S3Handler) S3List(writer http.ResponseWriter, request *http.Request) {
	_, err := writer.Write([]byte("test"))
	if err != nil {
		panic(fmt.Sprint("Error %s", err))
	}
	return
}