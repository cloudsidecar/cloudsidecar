package object

import (
	"cloud.google.com/go/storage"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	uuid2 "github.com/google/uuid"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"os"
	s3_handler "sidecar/pkg/aws/handler/s3"
	"sidecar/pkg/converter"
	"sidecar/pkg/logging"
	"sidecar/pkg/response_type"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Handler struct {
	*s3_handler.Handler
	fileMutex sync.Mutex
}

type Bucket interface {
	HeadHandle(writer http.ResponseWriter, request *http.Request)
	HeadParseInput(r *http.Request) (*s3.HeadObjectInput, error)
	GetHandle(writer http.ResponseWriter, request *http.Request)
	GetParseInput(r *http.Request) (*s3.GetObjectInput, error)
	PutHandle(writer http.ResponseWriter, request *http.Request)
	PutParseInput(r *http.Request) (*s3manager.UploadInput, error)
	MultiPartHandle(writer http.ResponseWriter, request *http.Request)
	MultiPartParseInput(r *http.Request) (*s3.CreateMultipartUploadInput, error)
	UploadPartHandle(writer http.ResponseWriter, request *http.Request)
	UploadPartParseInput(r *http.Request) (*s3.UploadPartInput, error)
	CompleteMultiPartHandle(writer http.ResponseWriter, request *http.Request)
	CompleteMultiPartParseInput(r *http.Request) (*s3.CompleteMultipartUploadInput, error)
	CopyHandle(writer http.ResponseWriter, request *http.Request)
	CopyParseInput(r *http.Request) (*s3.CopyObjectInput, error)
	DeleteHandle(writer http.ResponseWriter, request *http.Request)
	DeleteParseInput(r *http.Request) (*s3.DeleteObjectInput, error)
	MultiDeleteHandle(writer http.ResponseWriter, request *http.Request)
	MultiDeleteParseInput(r *http.Request) (*s3.DeleteObjectsInput, error)
	New(s3Handler *s3_handler.Handler) Handler
	Register(mux *mux.Router)
}

func (handler *Handler) Register(mux *mux.Router) {
	keyFromUrl := handler.Config.Get("gcp_destination_config.key_from_url")
	if keyFromUrl != nil && keyFromUrl == true{
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.HeadHandle).Methods("HEAD")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.GetHandle).Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}", handler.MultiDeleteHandle).Queries("delete", "").Methods("POST")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.MultiPartHandle).Queries("uploads", "").Methods("POST")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.UploadPartHandle).Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").Methods("PUT")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.CompleteMultiPartHandle).Queries("uploadId", "{uploadId}").Methods("POST")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.CopyHandle).Headers("x-amz-copy-source", "").Methods("PUT")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.PutHandle).Methods("PUT")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.DeleteHandle).Methods("DELETE")
	} else {
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.HeadHandle).Methods("HEAD")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.GetHandle).Methods("GET")
		mux.HandleFunc("/{bucket}", handler.MultiDeleteHandle).Queries("delete", "").Methods("POST")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.MultiPartHandle).Queries("uploads", "").Methods("POST")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.UploadPartHandle).Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").Methods("PUT")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.CompleteMultiPartHandle).Queries("uploadId", "{uploadId}").Methods("POST")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.CopyHandle).Headers("x-amz-copy-source", "").Methods("PUT")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.PutHandle).Methods("PUT")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.DeleteHandle).Methods("DELETE")
	}
}

func (handler *Handler) CompleteMultiPartHandle(writer http.ResponseWriter, request *http.Request){
	s3Req, _ := handler.CompleteMultiPartParseInput(request)
	var resp *response_type.CompleteMultipartUploadResult
	var err error
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		path := fmt.Sprintf("%s/%s", handler.Config.GetString("gcp_destination_config.gcs_config.multipart_db_directory"), *s3Req.UploadId)
		f, fileErr := os.Open(path)
		if fileErr != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error a %s %s", request.RequestURI, fileErr)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		defer f.Close()
		objects := make([]*storage.ObjectHandle, 0)
		sort.Slice(s3Req.MultipartUpload.Parts, func(i, j int) bool {
			return *s3Req.MultipartUpload.Parts[i].PartNumber < *s3Req.MultipartUpload.Parts[j].PartNumber
		})
		/*
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			pieces := strings.SplitN(scanner.Text(), ",", 2)
			pieceKey := pieces[1]
			objects = append(objects, handler.GCPClient.Bucket(*s3Req.Bucket).Object(pieceKey))
		}
		*/
		bucket := handler.GCPClientToBucket(*s3Req.Bucket, handler.GCPClient)
		for _, part := range s3Req.MultipartUpload.Parts {
			partNumber := *part.PartNumber
			key := partFileName(*s3Req.Key, partNumber)
			logging.Log.Info("Part number ", partNumber, " ", key)
			objects = append(objects, bucket.Object(key))
		}

		gResp, _ := handler.GCPBucketToObject(*s3Req.Key, bucket).ComposerFrom(objects...).Run(*handler.Context)
		resp = converter.GCSAttrToCombine(gResp)
		for _, object := range objects {
			object.Delete(*handler.Context)
		}
		defer os.Remove(path)
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.CompleteMultipartUploadRequest(s3Req)
		var s3Resp *s3.CompleteMultipartUploadOutput
		s3Resp, err = req.Send()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		resp = &response_type.CompleteMultipartUploadResult{
			Bucket: s3Resp.Bucket,
			Key: s3Resp.Key,
			ETag: s3Resp.ETag,
			Location: s3Resp.Location,
		}
	}
	if err != nil {
		writer.WriteHeader(404)
		logging.Log.Error("Error %s %s", request.RequestURI, err)
		writer.Write([]byte(string(fmt.Sprint(err))))
		return
	}
	output, _ := xml.MarshalIndent(resp, "  ", "    ")
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (handler *Handler) CompleteMultiPartParseInput(r *http.Request) (*s3.CompleteMultipartUploadInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	uploadId := vars["uploadId"]
	s3Req := &s3.CompleteMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
		UploadId: &uploadId,
	}
	bodyBytes := make([]byte, 64000)
	io.ReadFull(r.Body, bodyBytes)
	var input response_type.CompleteMultipartUploadInput
	xml.Unmarshal(bodyBytes, &input)
	logging.Log.Info("", input.Parts)
	s3Req.MultipartUpload = &s3.CompletedMultipartUpload{
		Parts: make([]s3.CompletedPart, len(input.Parts)),
	}
	for i, part := range input.Parts {
		s3Req.MultipartUpload.Parts[i] = s3.CompletedPart{
			ETag: part.ETag,
			PartNumber: part.PartNumber,
		}
	}
	return s3Req, nil
}

func partFileName(key string, part int64) string {
	return fmt.Sprintf("%s-part-%d", key, part)
}

func (handler *Handler) UploadPartHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.UploadPartParseInput(request)
	var resp *s3.UploadPartOutput
	var err error
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		key := partFileName(*s3Req.Key, *s3Req.PartNumber)
		bucket := handler.GCPClientToBucket(*s3Req.Bucket, handler.GCPClient)
		uploader := handler.GCPBucketToObject(key, bucket).NewWriter(*handler.Context)
		gReq, _ := handler.PutParseInput(request)
		_, err := converter.GCPUpload(gReq, uploader)
		uploader.Close()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		attrs, _ := handler.GCPBucketToObject(key, bucket).Attrs(*handler.Context)
		converter.GCSAttrToHeaders(attrs, writer)
		path := fmt.Sprintf("%s/%s", handler.Config.GetString("gcp_destination_config.gcs_config.multipart_db_directory"), *s3Req.UploadId)
		logging.Log.Info(path)
		handler.fileMutex.Lock()
		f, fileErr := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if fileErr != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, fileErr)
			writer.Write([]byte(string(fmt.Sprint(fileErr))))
			handler.fileMutex.Unlock()
			return
		}
		defer f.Close()
		_, fileErr = f.WriteString(fmt.Sprintf("%s,%s\n", writer.Header().Get("ETag"), key))
		handler.fileMutex.Unlock()
		if fileErr != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, fileErr)
			writer.Write([]byte(string(fmt.Sprint(fileErr))))
			return
		}
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.UploadPartRequest(s3Req)
		req.HTTPRequest.Header.Set("Content-Length", request.Header.Get("Content-Length"))
		req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", request.Header.Get("X-Amz-Content-Sha256"))
		req.Body = aws.ReadSeekCloser(request.Body)
		resp, err = req.Send()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error in uploading %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		logging.Log.Info(fmt.Sprint(resp))
		if header := resp.ETag; header != nil {
			writer.Header().Set("ETag", *header)
		}
	}
	writer.WriteHeader(200)
	writer.Write([]byte(""))
}
func (handler *Handler) UploadPartParseInput(r *http.Request) (*s3.UploadPartInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	partNumber, err := strconv.ParseInt(vars["partNumber"], 10, 64)
	uploadId := vars["uploadId"]
	return &s3.UploadPartInput{
		Bucket: &bucket,
		Key: &key,
		PartNumber: &partNumber,
		UploadId: &uploadId,
	}, err
}

func (handler *Handler) MultiPartHandle(writer http.ResponseWriter, request *http.Request){
	s3Req, _ := handler.MultiPartParseInput(request)
	var resp *s3.CreateMultipartUploadOutput
	var err error
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		uuid := uuid2.New().String()
		path := fmt.Sprintf("%s/%s", handler.Config.GetString("gcp_destination_config.gcs_config.multipart_db_directory"), uuid)
		f, fileErr := os.Create(path)
		if fileErr != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		defer f.Close()
		logging.Log.Info(uuid)
		resp = &s3.CreateMultipartUploadOutput{
			Key: s3Req.Key,
			Bucket: s3Req.Bucket,
			UploadId: &uuid,
		}
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.CreateMultipartUploadRequest(s3Req)
		resp, err = req.Send()
	}
	if err != nil {
		writer.WriteHeader(404)
		logging.Log.Error("Error %s %s", request.RequestURI, err)
		writer.Write([]byte(string(fmt.Sprint(err))))
		return
	}
	output, _ := xml.MarshalIndent(resp, "  ", "    ")
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (handler *Handler) MultiPartParseInput(r *http.Request) (*s3.CreateMultipartUploadInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	}
	return s3Req, nil
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
		logging.Log.Debug("CHUNKED %d", contentLength)
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
		handler.GCPRequestSetup(request)
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, handler.GCPClient)
		uploader := handler.GCPBucketToObject(*s3Req.Key, bucketHandle).NewWriter(*handler.Context)
		defer uploader.Close()
		_, err := converter.GCPUpload(s3Req, uploader)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
	} else {
		logging.LogUsingAWS()
		uploader := s3manager.NewUploaderWithClient(handler.S3Client)
		_, err = uploader.Upload(s3Req)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
	}
	writer.WriteHeader(200)
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
		handler.GCPRequestSetup(request)
		bucket := handler.BucketRename(*input.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, handler.GCPClient)
		objHandle := handler.GCPBucketToObject(*input.Key, bucketHandle)
		attrs, err := objHandle.Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
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
			logging.Log.Error("Error %s %s", request.RequestURI, readerError)
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
		logging.LogUsingAWS()
		req := handler.S3Client.GetObjectRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, respError)
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
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		bucket := handler.BucketRename(*input.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, handler.GCPClient)
		resp, err := handler.GCPBucketToObject(*input.Key, bucketHandle).Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		converter.GCSAttrToHeaders(resp, writer)
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.HeadObjectRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, respError)
			return
		}
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

func (handler *Handler) CopyParseInput(r *http.Request) (*s3.CopyObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	source := r.Header.Get("x-amz-copy-source")
	s3Req := &s3.CopyObjectInput{
		Bucket: &bucket,
		Key: &key,
		CopySource: &source,
	}
	if header := r.Header.Get("Content-Type"); header != "" {
		s3Req.ContentType = &header
	}
	return s3Req, nil
}

func (handler *Handler) CopyHandle(writer http.ResponseWriter, request *http.Request){
	s3Req, _ := handler.CopyParseInput(request)
	var copyResult response_type.CopyResult
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		source := *s3Req.CopySource
		if strings.Index(source, "/") == 0 {
			source = source[1:]
		}
		sourcePieces := strings.SplitN(source, "/", 2)
		sourceBucket := sourcePieces[0]
		sourceKey := sourcePieces[1]
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, handler.GCPClient)
		sourceBucket = handler.BucketRename(sourceBucket)
		sourceHandle := handler.GCPClientToBucket(sourceBucket, handler.GCPClient).Object(sourceKey)
		uploader := handler.GCPBucketToObject(*s3Req.Key, bucketHandle).CopierFrom(sourceHandle)
		attrs, err := uploader.Run(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		copyResult = converter.GCSCopyResponseToAWS(attrs)
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.CopyObjectRequest(s3Req)
		result, err := req.Send()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		copyResult = response_type.CopyResult{
			LastModified: converter.FormatTimeZulu(result.CopyObjectResult.LastModified),
			ETag: *result.CopyObjectResult.ETag,
		}
	}
	output, _ := xml.MarshalIndent(copyResult, "  ", "    ")
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (handler *Handler) DeleteParseInput(r *http.Request) (*s3.DeleteObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key: &key,
	}
	return s3Req, nil
}

func (handler *Handler) DeleteHandle(writer http.ResponseWriter, request *http.Request){
	s3Req, _ := handler.DeleteParseInput(request)
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, handler.GCPClient)
		objectHandle := handler.GCPBucketToObject(*s3Req.Key, bucketHandle)
		err := objectHandle.Delete(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.DeleteObjectRequest(s3Req)
		_, err := req.Send()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
	}
	writer.WriteHeader(200)
}

func (handler *Handler) MultiDeleteParseInput(r *http.Request) (*s3.DeleteObjectsInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	length, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}
	bodyBytes := make([]byte, length)
	_, err = io.ReadFull(r.Body, bodyBytes)
	if err != nil {
		return nil, err
	}
	var input response_type.MultiDeleteRequest
	err = xml.Unmarshal(bodyBytes, &input)
	if err != nil {
		return nil, err
	}
	keys := make([]s3.ObjectIdentifier, len(input.Objects))
	for i, key := range input.Objects {
		keys[i] = s3.ObjectIdentifier{
			Key: key.Key,
		}
	}
	s3Req := &s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &s3.Delete{
			Objects: keys,
		},
	}
	return s3Req, nil
}

func (handler *Handler) MultiDeleteHandle(writer http.ResponseWriter, request *http.Request){
	s3Req, _ := handler.MultiDeleteParseInput(request)
	response := response_type.MultiDeleteResult{
	}
	if handler.GCPClient != nil {
		handler.GCPRequestSetup(request)
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, handler.GCPClient)
		deletedKeys := make([]*string, 0)
		failedKeys := make([]*string, 0)
		for _, obj := range s3Req.Delete.Objects {
			err := bucketHandle.Object(*obj.Key).Delete(*handler.Context)
			if err != nil {
				failedKeys = append(failedKeys, obj.Key)
			} else {
				deletedKeys = append(deletedKeys, obj.Key)
			}
		}
		deletedObjects := make([]*response_type.DeleteObject, len(deletedKeys))
		for i, obj := range deletedKeys {
			deletedObjects[i] = &response_type.DeleteObject{
				Key: obj,
			}
		}
		response.Objects = deletedObjects
		failedObjects := make([]*response_type.ErrorResult, len(failedKeys))
		for i, obj := range failedKeys {
			failedObjects[i] = &response_type.ErrorResult{
				Key: obj,
			}
		}
		response.Errors = failedObjects
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.DeleteObjectsRequest(s3Req)
		resp, err := req.Send()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		deletedObjects := make([]*response_type.DeleteObject, len(resp.Deleted))
		for i, obj := range resp.Deleted {
			deletedObjects[i] = &response_type.DeleteObject{
				Key: obj.Key,
			}
		}
		response.Objects = deletedObjects
		failedObjects := make([]*response_type.ErrorResult, len(resp.Errors))
		for i, obj := range resp.Errors {
			failedObjects[i] = &response_type.ErrorResult{
				Key: obj.Key,
				Code: obj.Code,
				Message: obj.Message,
			}
		}
		response.Errors = failedObjects
	}
	output, _ := xml.MarshalIndent(response, "  ", "    ")
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}

func New(s3Handler *s3_handler.Handler) *Handler {
	return &Handler{s3Handler, sync.Mutex{}}
}
