package object

import (
	"cloudsidecar/pkg/logging"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	gcs_handler "cloudsidecar/pkg/gcp/handler/gcs"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/gorilla/mux"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Handler struct {
	*gcs_handler.Handler
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
	New(s3Handler *gcs_handler.Handler) Handler
	Register(mux *mux.Router)
}

// Register HTTP patterns to functions
func (wrapper *Handler) Register(mux *mux.Router) {
	logging.Log.Infof("Stuff %s", wrapper.Config)
	keyFromUrl := wrapper.Config.Get("gcp_destination_config.key_from_url")
	if keyFromUrl != nil && keyFromUrl == true {
		mux.HandleFunc("/{creds}/storage/v1/b/{bucket}/o/{key:[^#?\\s]+}", wrapper.GetHandle).Methods("GET")
		mux.HandleFunc("/{creds}/download/storage/v1/b/{bucket}/o/{key:[^#?\\s]+}", wrapper.GetHandle).Methods("GET")
	} else {
		mux.HandleFunc("/storage/v1/b/{bucket}/o/{key:[^#?\\s]+}", wrapper.GetHandle).Methods("GET")
		mux.HandleFunc("/download/storage/v1/b/{bucket}/o/{key:[^#?\\s]+}", wrapper.GetHandle).Methods("GET")
	}
}

func (handler *Handler) GetHandle(writer http.ResponseWriter, request *http.Request) {
	input, _ := handler.GetParseInput(request)
	identifier := rand.Int()
	if handler.Config.IsSet("aws_destination_config") {
		logging.LogUsingAWS()
		bucket := handler.BucketRename(*input.Bucket)
		input.Bucket = &bucket
		req := handler.S3Client.GetObjectRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", identifier, request.RequestURI, respError)
			return
		}
		defer resp.Body.Close()
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
		if n, writeErr := io.Copy(writer, resp.Body); writeErr != nil {
			logging.Log.Error("Some error writing", n, identifier, request.RequestURI, writeErr)
		}
	} else if handler.Config.IsSet("gcp_destination_config") {
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
		bucketHandle := handler.GCPClientToBucket(*input.Bucket, client)
		objHandle := handler.GCPBucketToObject(*input.Key, bucketHandle)
		reader, readerError := objHandle.NewReader(*handler.Context)
		if readerError != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, readerError)
			return
		}
		defer reader.Close()
		if n, writeErr := io.Copy(writer, reader); writeErr != nil {
			logging.Log.Error("Some error writing", n, identifier, request.RequestURI, writeErr)
			return
		}
		logging.Log.Info("Finish GET request", identifier, request.RequestURI, request.Header.Get("Range"))
	}
}
func (handler *Handler) GetParseInput(r *http.Request) (*s3.GetObjectInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	return &s3.GetObjectInput{Bucket: &bucket, Key: &key}, nil
}

func New(gcsHandler *gcs_handler.Handler) *Handler {
	return &Handler{gcsHandler, sync.Mutex{}}
}
