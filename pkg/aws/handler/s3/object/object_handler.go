// Handle s3 object style requests

package object

import (
	"cloud.google.com/go/storage"
	s3_handler "cloudsidecar/pkg/aws/handler/s3"
	"cloudsidecar/pkg/converter"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	uuid2 "github.com/google/uuid"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
	"io"
	"math/rand"
	"net/http"
	"os"
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

// Interface for object functions
type Object interface {
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

// Register all HTTP handlers
func (handler *Handler) Register(mux *mux.Router) {
	keyFromUrl := handler.Config.Get("gcp_destination_config.key_from_url")
	if keyFromUrl != nil && keyFromUrl == true {
		// Credits will be pased in URL instead of config
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.HeadHandle).Methods("HEAD")
		mux.HandleFunc("/{creds}/{bucket}/{key:[^#?\\s]+}", handler.GetHandle).Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}", handler.MultiDeleteHandle).Queries("delete", "").Methods("POST")
		mux.HandleFunc("/{creds}/{bucket}/", handler.MultiDeleteHandle).Queries("delete", "").Methods("POST")
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
		mux.HandleFunc("/{bucket}/", handler.MultiDeleteHandle).Queries("delete", "").Methods("POST")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.MultiPartHandle).Queries("uploads", "").Methods("POST")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.UploadPartHandle).Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").Methods("PUT")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.CompleteMultiPartHandle).Queries("uploadId", "{uploadId}").Methods("POST")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.CopyHandle).Headers("x-amz-copy-source", "").Methods("PUT")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.PutHandle).Methods("PUT")
		mux.HandleFunc("/{bucket}/{key:[^#?\\s]+}", handler.DeleteHandle).Methods("DELETE")
	}
}

func writeInternalError(writer http.ResponseWriter, message string) {
	code := "InternalError"
	xmlResponse := response_type.AWSACLResponseError{
		Code:    &code,
		Message: &message,
	}
	output, _ := xml.Marshal(xmlResponse)
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write(output)
}

func filesAsString(objects []*storage.ObjectHandle) string {
	names := make([]string, len(objects))
	for i, obj := range objects {
		names[i] = obj.ObjectName()
	}
	return strings.Join(names, ", ")
}

func (handler *Handler) doCombine(bucket s3_handler.GCPBucket, target string, objects []*storage.ObjectHandle) (*storage.ObjectAttrs, error) {
	objectCount := len(objects)
	var toCombine []*storage.ObjectHandle
	maxSize := 32
	if objectCount > maxSize {
		toCombine = make([]*storage.ObjectHandle, 2)
		pos := objectCount / 2
		firstTarget := fmt.Sprintf("%s_1", target)
		firstHandle, err := handler.doCombine(bucket, firstTarget, objects[:pos])
		if err != nil {
			return nil, err
		}
		toCombine[0] = bucket.Object(firstHandle.Name)

		secondTarget := fmt.Sprintf("%s_2", target)
		secondHandle, err := handler.doCombine(bucket, secondTarget, objects[pos:])
		if err != nil {
			return nil, err
		}
		toCombine[1] = bucket.Object(secondHandle.Name)
	} else {
		toCombine = objects
	}
	logging.Log.Debugf("Combining to %s %v", target, filesAsString(toCombine))
	gResp, err := handler.GCPBucketToObject(target, bucket).ComposerFrom(toCombine...).Run(*handler.Context)
	if err != nil {
		return nil, err
	}
	for _, object := range toCombine {
		logging.Log.Debugf("Deleting %s", object.ObjectName())
		if deleteErr := handler.deleteGCPObjectWithRetry(object); deleteErr != nil {
			return nil, deleteErr
		}
	}
	return gResp, nil
}

// Handle completing multipart upload
func (handler *Handler) CompleteMultiPartHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.CompleteMultiPartParseInput(request)
	var resp *response_type.CompleteMultipartUploadResult
	var err error
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return gcp client after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writeInternalError(writer, err.Error())
			return
		}
		path := fmt.Sprintf("%s/%s", handler.Config.GetString("gcp_destination_config.gcs_config.multipart_db_directory"), *s3Req.UploadId)
		// Read file that stored locations of parts
		f, fileErr := os.Open(path)
		if fileErr != nil {
			f.Close()
			writer.WriteHeader(404)
			logging.Log.Error("Error a %s %s", request.RequestURI, fileErr)
			writeInternalError(writer, fileErr.Error())
			return
		}
		f.Close()
		objects := make([]*storage.ObjectHandle, 0)
		sort.Slice(s3Req.MultipartUpload.Parts, func(i, j int) bool {
			return *s3Req.MultipartUpload.Parts[i].PartNumber < *s3Req.MultipartUpload.Parts[j].PartNumber
		})
		bucket := handler.GCPClientToBucket(*s3Req.Bucket, client)
		for _, part := range s3Req.MultipartUpload.Parts {
			partNumber := *part.PartNumber
			key := partFileName(*s3Req.Key, partNumber, handler.Config.GetString("gcp_destination_config.gcs_config.multipart_temp_file_prefix"))
			logging.Log.Info("Part number ", partNumber, " ", key)
			objects = append(objects, bucket.Object(key))
		}

		// Join pieces
		gResp, err := handler.doCombine(bucket, *s3Req.Key, objects)
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			writeInternalError(writer, err.Error())
			return
		}
		resp = converter.GCSAttrToCombine(gResp)
		defer os.Remove(path)
		logging.Log.Infof("Finished multipart upload {}", *s3Req.UploadId)
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
			Bucket:   s3Resp.Bucket,
			Key:      s3Resp.Key,
			ETag:     s3Resp.ETag,
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

// Parse input for complete multipart upload command
func (handler *Handler) CompleteMultiPartParseInput(r *http.Request) (*s3.CompleteMultipartUploadInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	uploadId := vars["uploadId"]
	s3Req := &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadId,
	}
	bodyBytes := make([]byte, 64000)
	io.ReadFull(r.Body, bodyBytes)
	defer r.Body.Close()
	var input response_type.CompleteMultipartUploadInput
	xml.Unmarshal(bodyBytes, &input)
	logging.Log.Info("", input.Parts)
	s3Req.MultipartUpload = &s3.CompletedMultipartUpload{
		Parts: make([]s3.CompletedPart, len(input.Parts)),
	}
	for i, part := range input.Parts {
		s3Req.MultipartUpload.Parts[i] = s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
	}
	return s3Req, nil
}

func partFileName(key string, part int64, prefix string) string {
	if prefix != "" {
		keyParts := strings.Split(key, "/")
		keyParts[len(keyParts) - 1] = prefix + keyParts[len(keyParts) - 1]
		key = strings.Join(keyParts, "/")
	}
	return fmt.Sprintf("%s-part-%d", key, part)
}

// Delete gcp object and retry on failure
func (handler *Handler) deleteGCPObjectWithRetry(object s3_handler.GCPObject) error {
	errors := retry.Do(
		func() error {
			err := object.Delete(*handler.Context)
			return err
		},
		retry.OnRetry(func(n uint, err error) {
			logging.Log.Warning("GCP Delete Retry #%d: %s\n", n, err)
		}),
		retry.Delay(1*time.Second),
	)
	return errors
}

// Get gcp attributes of object and retry on failure
func (handler *Handler) getGCPAttrsWithRetry(object s3_handler.GCPObject) (attrs *storage.ObjectAttrs, err error) {
	var attributes *storage.ObjectAttrs
	errors := retry.Do(
		func() error {
			attributes, err = object.Attrs(*handler.Context)
			return err
		},
		retry.OnRetry(func(n uint, err error) {
			logging.Log.Warning("GCP Attrs Retry #%d: %s\n", n, err)
		}),
		retry.Delay(1*time.Second),
	)
	return attributes, errors
}

// Handle uploading part of a multipart file
func (handler *Handler) UploadPartHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.UploadPartParseInput(request)
	var resp *s3.UploadPartOutput
	var err error
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		key := partFileName(*s3Req.Key, *s3Req.PartNumber, handler.Config.GetString("gcp_destination_config.gcs_config.multipart_temp_file_prefix"))
		bucket := handler.GCPClientToBucket(*s3Req.Bucket, client)
		obj := handler.GCPBucketToObject(key, bucket)
		uploader := handler.GCPObjectToWriter(obj, *handler.Context)
		gReq, _ := handler.PutParseInput(request)
		_, err = converter.GCPUpload(gReq, uploader)
		closeErr := uploader.Close()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		if closeErr != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error %s %s %s %s", request.RequestURI, bucket, key, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		attrs := uploader.Attrs()
		converter.GCSMD5ToEtag(attrs, writer)
		path := fmt.Sprintf("%s/%s", handler.Config.GetString("gcp_destination_config.gcs_config.multipart_db_directory"), *s3Req.UploadId)
		logging.Log.Info("Temp upload parts file path " + path)
		// Add part file to multipart temporary file
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

// Parse input for multipart upload
func (handler *Handler) UploadPartParseInput(r *http.Request) (*s3.UploadPartInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	partNumber, err := strconv.ParseInt(vars["partNumber"], 10, 64)
	uploadId := vars["uploadId"]
	return &s3.UploadPartInput{
		Bucket:     &bucket,
		Key:        &key,
		PartNumber: &partNumber,
		UploadId:   &uploadId,
	}, err
}

// Handle request to create multipart upload
func (handler *Handler) MultiPartHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.MultiPartParseInput(request)
	var resp *response_type.InitiateMultipartUploadResult
	var createResp *s3.CreateMultipartUploadOutput
	var err error

	if handler.Config.IsSet("gcp_destination_config") {
		// GCS, so create a temporary local file to store parts.  This file is used to join parts later
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
		resp = &response_type.InitiateMultipartUploadResult{
			Key:      s3Req.Key,
			Bucket:   s3Req.Bucket,
			UploadId: &uuid,
			XmlNS:    response_type.ACLXmlNs,
		}
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.CreateMultipartUploadRequest(s3Req)
		createResp, err = req.Send()
		resp = &response_type.InitiateMultipartUploadResult{
			Key:      s3Req.Key,
			Bucket:   s3Req.Bucket,
			UploadId: createResp.UploadId,
			XmlNS:    response_type.ACLXmlNs,
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

// Parse request for creating a multipart upload
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

// Parse request for upload
func (handler *Handler) PutParseInput(r *http.Request) (*s3manager.UploadInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
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
			Reader:            &r.Body,
			ChunkNextPosition: -1,
		}
		s3Req.Body = &readerWrapper
	}
	return s3Req, nil
}

// Handle an upload
func (handler *Handler) PutHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.PutParseInput(request)
	var err error
	defer request.Body.Close()
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		logging.Log.Info("Begin PUT request", request.RequestURI)
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		uploader := handler.GCPBucketToObject(*s3Req.Key, bucketHandle).NewWriter(*handler.Context)
		_, err = converter.GCPUpload(s3Req, uploader)
		uploaderErr := uploader.Close()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		if uploaderErr != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		attrs := uploader.Attrs()
		converter.GCSMD5ToEtag(attrs, writer)
		logging.Log.Info("Finish PUT request", request.RequestURI)
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

// Parse request for GET
func (handler *Handler) GetParseInput(r *http.Request) (*s3.GetObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	return &s3.GetObjectInput{Bucket: &bucket, Key: &key}, nil
}

// Handle get request
func (handler *Handler) GetHandle(writer http.ResponseWriter, request *http.Request) {
	input, _ := handler.GetParseInput(request)
	identifier := rand.Int()
	if header := request.Header.Get("Range"); header != "" {
		input.Range = &header
	}
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		logging.Log.Info("Begin GET request", identifier, request.RequestURI, request.Header.Get("Range"))
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := handler.BucketRename(*input.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		objHandle := handler.GCPBucketToObject(*input.Key, bucketHandle)
		// Need to get attributes first to get size
		attrs, err := objHandle.Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		var reader *storage.Reader
		var readerError error
		if input.Range != nil {
			// Range requests are by length not by start and end
			equalSplit := strings.SplitN(*input.Range, "=", 2)
			byteSplit := strings.SplitN(equalSplit[1], "-", 2)
			startByte, _ := strconv.ParseInt(byteSplit[0], 10, 64)
			length := int64(-1)
			if len(byteSplit) > 1 {
				endByte, _ := strconv.ParseInt(byteSplit[1], 10, 64)
				length = endByte + 1 - startByte
				attrs.Size = length
			} else {
				length = attrs.Size - startByte
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
		// Send headers
		converter.GCSAttrToHeaders(attrs, writer)
		defer reader.Close()
		if n, writeErr := io.Copy(writer, reader); writeErr != nil {
			logging.Log.Error("Some error writing", n, identifier, request.RequestURI, writeErr)
			return
		}
		logging.Log.Info("Finish GET request", identifier, request.RequestURI, request.Header.Get("Range"))
	} else {
		logging.LogUsingAWS()
		req := handler.S3Client.GetObjectRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", identifier, request.RequestURI, respError)
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
		if n, writeErr := io.Copy(writer, resp.Body); writeErr != nil {
			logging.Log.Error("Some error writing", n, identifier, request.RequestURI, writeErr)
		}
	}
}

// Parse input for HEAD request
func (handler *Handler) HeadParseInput(r *http.Request) (*s3.HeadObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	return &s3.HeadObjectInput{Bucket: &bucket, Key: &key}, nil
}

// Handle HEAD request
func (handler *Handler) HeadHandle(writer http.ResponseWriter, request *http.Request) {
	input, _ := handler.HeadParseInput(request)
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		var resp *storage.ObjectAttrs
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := handler.BucketRename(*input.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		if strings.HasSuffix(*input.Key, "/") {
			//directories dont have header info so fake it
			it := bucketHandle.Objects(*handler.Context, &storage.Query{
				Delimiter: "/",
				Prefix:    *input.Key,
				Versions:  false,
			})
			var pageResponse []*storage.ObjectAttrs
			_, err := iterator.NewPager(it, 1, "").NextPage(&pageResponse)
			if err != nil {
				writer.WriteHeader(404)
				logging.Log.Error("Error %s %s", request.RequestURI, err)
				return
			}
			if len(pageResponse) == 0 {
				writer.WriteHeader(404)
				logging.Log.Error("Error %s key doesn't exist", request.RequestURI)
				return
			}
			hash := md5.Sum([]byte(time.Now().String()))
			resp = &storage.ObjectAttrs{
				Bucket:  *input.Bucket,
				Name:    *input.Key,
				Updated: time.Now(),
				MD5:     hash[:],
			}
		} else {
			var err error
			resp, err = handler.GCPBucketToObject(*input.Key, bucketHandle).Attrs(*handler.Context)
			if err != nil {
				writer.WriteHeader(404)
				logging.Log.Error("Error %s %s", request.RequestURI, err)
				return
			}
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

// Parse input for copy command
func (handler *Handler) CopyParseInput(r *http.Request) (*s3.CopyObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	source := r.Header.Get("x-amz-copy-source")
	s3Req := &s3.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &key,
		CopySource: &source,
	}
	if header := r.Header.Get("Content-Type"); header != "" {
		s3Req.ContentType = &header
	}
	return s3Req, nil
}

// Handle copy command
func (handler *Handler) CopyHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.CopyParseInput(request)
	var copyResult response_type.CopyResult
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		source := *s3Req.CopySource
		if strings.Index(source, "/") == 0 {
			source = source[1:]
		}
		sourcePieces := strings.SplitN(source, "/", 2)
		sourceBucket := sourcePieces[0]
		sourceKey := sourcePieces[1]
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		sourceBucket = handler.BucketRename(sourceBucket)
		sourceHandle := handler.GCPClientToBucket(sourceBucket, client).Object(sourceKey)
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
			ETag:         *result.CopyObjectResult.ETag,
		}
	}
	output, _ := xml.MarshalIndent(copyResult, "  ", "    ")
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}

// Parse input for delete
func (handler *Handler) DeleteParseInput(r *http.Request) (*s3.DeleteObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	return s3Req, nil
}

// Handle delete operation
func (handler *Handler) DeleteHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.DeleteParseInput(request)
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		objectHandle := handler.GCPBucketToObject(*s3Req.Key, bucketHandle)
		err = objectHandle.Delete(*handler.Context)
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

// Parse input for multi delete operation
func (handler *Handler) MultiDeleteParseInput(r *http.Request) (*s3.DeleteObjectsInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	length, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}
	bodyBytes := make([]byte, length)
	_, err = io.ReadFull(r.Body, bodyBytes)
	defer r.Body.Close()
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

// Handle request for deleting multiple files
func (handler *Handler) MultiDeleteHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, _ := handler.MultiDeleteParseInput(request)
	response := response_type.MultiDeleteResult{}
	if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		deletedKeys := make([]string, 0)
		failedKeys := make([]string, 0)
		// iterate through objects and delete each
		for _, obj := range s3Req.Delete.Objects {
			err := bucketHandle.Object(*obj.Key).Delete(*handler.Context)
			if err != nil {
				failedKeys = append(failedKeys, *obj.Key)
			} else {
				deletedKeys = append(deletedKeys, *obj.Key)
			}
		}
		// aws never returns failed deletes
		logging.Log.Debugf("failed keys %s succeeded keys %s", failedKeys, deletedKeys)
		deletedObjects := make([]*response_type.DeleteObject, len(deletedKeys))
		for i, obj := range deletedKeys {
			deletedObjects[i] = &response_type.DeleteObject{
				Key: &obj,
			}
		}
		response.Objects = deletedObjects
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
				Key:     obj.Key,
				Code:    obj.Code,
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
