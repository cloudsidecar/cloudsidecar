package object

import (
	"cloud.google.com/go/storage"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	s3_handler "sidecar/aws/handler/s3"
	"sidecar/converter"
	"strconv"
	"strings"
	"time"
)

type Handler struct {
	*s3_handler.Handler
}

type Bucket interface {
	HeadHandle(writer http.ResponseWriter, request *http.Request)
	HeadParseInput(r *http.Request) (*s3.HeadObjectInput, error)
	GetHandle(writer http.ResponseWriter, request *http.Request)
	GetParseInput(r *http.Request) (*s3.GetObjectInput, error)
	PutHandle(writer http.ResponseWriter, request *http.Request)
	PutParseInput(r *http.Request) (*s3manager.UploadInput, error)
	New(s3Handler *s3_handler.Handler) Handler
}

func (handler *Handler) PutParseInput(r *http.Request) (*s3manager.UploadInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3manager.UploadInput{
		Bucket: &bucket,
		Key: &key,
	}
	if header := r.Header.Get("Content-MD5"); header != "" {
		s3Req.ContentMD5 = &header
	}
	if header := r.Header.Get("Content-Type"); header != "" {
		s3Req.ContentType = &header
	}
	isChunked := false
	if header := r.Header.Get("x-amz-content-sha256"); header == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		isChunked = true
	}
	var contentLength int64
	if header := r.Header.Get("x-amz-decoded-content-length"); header != "" {
		contentLength, _ = strconv.ParseInt(header, 10, 64)
	} else if header := r.Header.Get("x-amz-decoded-content-length"); header != "" {
		contentLength, _ = strconv.ParseInt(header, 10, 64)
	}
	if !isChunked {
		s3Req.Body = r.Body
	} else {
		fmt.Printf("CHUNKED %d", contentLength)
		readerWrapper := ChunkedReaderWrapper{
			Reader:         &r.Body,
			ChunkNextPosition: -1,
		}
		s3Req.Body = &readerWrapper
	}
	return s3Req, nil
}

func (handler *Handler) PutHandle(writer http.ResponseWriter, request *http.Request){
	s3Req, _ := handler.PutParseInput(request)
	var err error
	// wg := sync.WaitGroup{}
	defer request.Body.Close()
	if handler.GCPClient != nil {
		bucket := handler.BucketRename(*s3Req.Bucket)
		uploader := handler.GCPClient.Bucket(bucket).Object(*s3Req.Key).NewWriter(*handler.Context)
		defer uploader.Close()
		_, err := converter.GCPUpload(s3Req, uploader)
		if err != nil {
			fmt.Printf("\nBOOO Error %s\n", err)
		}
	} else {
		uploader := s3manager.NewUploaderWithClient(handler.S3Client)
		_, err = uploader.Upload(s3Req)
		if err != nil {
			fmt.Printf("Error %s", err)
		}
	}
	writer.WriteHeader(200)
	fmt.Printf("DONE")
	writer.Write([]byte(""))
}


func (handler *Handler) GetParseInput(r *http.Request) (*s3.GetObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	return &s3.GetObjectInput{Bucket: &bucket, Key: &key}, nil
}

func (handler *Handler) GetHandle(writer http.ResponseWriter, request *http.Request) {
	input, _ := handler.GetParseInput(request)
	if header := request.Header.Get("Range"); header != "" {
		input.Range = &header
	}
	if handler.GCPClient != nil {
		bucket := handler.BucketRename(*input.Bucket)
		objHandle := handler.GCPClient.Bucket(bucket).Object(*input.Key)
		attrs, err := objHandle.Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", err)
			return
		}
		converter.GCSAttrToHeaders(attrs, writer)
		var reader *storage.Reader
		var readerError error
		if input.Range != nil {
			equalSplit := strings.SplitN(*input.Range, "=", 2)
			byteSplit := strings.SplitN(equalSplit[1], "-", 2)
			startByte, _ := strconv.ParseInt(byteSplit[0], 10, 64)
			length := int64(-1)
			if len(byteSplit) > 1 {
				endByte, _ := strconv.ParseInt(byteSplit[1], 10, 64)
				length = endByte + 1 - startByte
			}
			reader, readerError = objHandle.NewRangeReader(*handler.Context, startByte, length)
		} else {
			reader, readerError = objHandle.NewReader(*handler.Context)
		}
		if readerError != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", readerError)
			return
		}
		defer reader.Close()
		buffer := make([]byte, 4096)
		for {
			n, err := reader.Read(buffer)
			if n > 0 {
				writer.Write(buffer[:n])
			}
			if err == io.EOF {
				break
			}
		}
	} else {
		req := handler.S3Client.GetObjectRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", respError)
			return
		}
		if header := resp.ServerSideEncryption; header != "" {
			writer.Header().Set("ServerSideEncryption", string(header))
		}
		if header := resp.LastModified; header != nil {
			lastMod := header.Format(time.RFC1123)
			lastMod = strings.Replace(lastMod, "UTC", "GMT", 1)
			writer.Header().Set("Last-Modified", lastMod)
		}
		if header := resp.ContentRange; header != nil {
			writer.Header().Set("ContentRange", *header)
		}
		if header := resp.ETag; header != nil {
			writer.Header().Set("ETag", *header)
		}
		if header := resp.ContentLength; header != nil {
			writer.Header().Set("Content-Length", strconv.FormatInt(*header, 10))
		}
		defer resp.Body.Close()
		buffer := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buffer)
			if n > 0 {
				writer.Write(buffer[:n])
			}
			if err == io.EOF {
				break
			}
		}
	}
	return
}

func (handler *Handler) HeadParseInput(r *http.Request) (*s3.HeadObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	return &s3.HeadObjectInput{Bucket: &bucket, Key: &key}, nil
}

func (handler *Handler) HeadHandle(writer http.ResponseWriter, request *http.Request) {
	input, _ := handler.HeadParseInput(request)
	fmt.Printf("Looking for %s\n", input)
	fmt.Printf("URL %s\n", request.URL)
	if handler.GCPClient != nil {
		bucket := handler.BucketRename(*input.Bucket)
		resp, err := handler.GCPClient.Bucket(bucket).Object(*input.Key).Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", err)
			return
		}
		fmt.Printf("Response %s\n", *resp)
		converter.GCSAttrToHeaders(resp, writer)
	} else {
		req := handler.S3Client.HeadObjectRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", respError)
			return
		}
		fmt.Printf("Response %s\n", resp.String())
		if resp.AcceptRanges != nil {
			writer.Header().Set("Accept-Ranges", *resp.AcceptRanges)
		}
		if resp.ContentLength != nil {
			writer.Header().Set("Content-Length", strconv.FormatInt(*resp.ContentLength, 10))
		}
		if resp.ServerSideEncryption != "" {
			writer.Header().Set("x-amz-server-side-encryption", string(resp.ServerSideEncryption))
		}
		if resp.CacheControl != nil {
			writer.Header().Set("Cache-Control", *resp.CacheControl)
		}
		if resp.ContentType != nil {
			writer.Header().Set("Content-Type", *resp.ContentType)
		}
		if resp.ETag != nil {
			writer.Header().Set("ETag", *resp.ETag)
		}
		if resp.LastModified != nil {
			lastMod := resp.LastModified.Format(time.RFC1123)
			lastMod = strings.Replace(lastMod, "UTC", "GMT", 1)
			writer.Header().Set("Last-Modified", lastMod)
		}
	}
	writer.WriteHeader(200)
	return
}

func New(s3Handler *s3_handler.Handler) *Handler {
	return &Handler{s3Handler}
}
