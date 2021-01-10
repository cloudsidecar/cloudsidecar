package object

import (
	"cloud.google.com/go/storage"
	"cloudsidecar/pkg/converter"
	gcs_handler "cloudsidecar/pkg/gcp/handler/gcs"
	"cloudsidecar/pkg/logging"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"math/rand"
	"mime"
	"mime/multipart"
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
	GetHandle(writer http.ResponseWriter, request *http.Request)
	GetParseInput(r *http.Request) (*s3.GetObjectInput, error)
	UploadMultipartHandle(writer http.ResponseWriter, request *http.Request)
	UploadMultipartParseInput(r *http.Request) (*s3manager.UploadInput, *multipart.Reader, error)
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
		mux.HandleFunc("/{creds}/upload/storage/v1/b/{bucket}/o", wrapper.UploadMultipartHandle).Queries("name", "{key}", "uploadType", "{uploadType}").Methods("POST")
		mux.HandleFunc("/{creds}/upload/storage/v1/b/{bucket}/o", wrapper.UploadMultipartHandle).Queries("uploadType", "{uploadType}").Methods("POST")
	} else {
		mux.HandleFunc("/storage/v1/b/{bucket}/o/{key:[^#?\\s]+}", wrapper.GetHandle).Methods("GET")
		mux.HandleFunc("/download/storage/v1/b/{bucket}/o/{key:[^#?\\s]+}", wrapper.GetHandle).Methods("GET")
		mux.HandleFunc("/upload/storage/v1/b/{bucket}/o", wrapper.UploadMultipartHandle).Queries("name", "{key}", "uploadType", "{uploadType}").Methods("POST")
		mux.HandleFunc("/upload/storage/v1/b/{bucket}/o", wrapper.UploadMultipartHandle).Queries("uploadType", "multipart").Methods("POST")
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

// Parse request for upload
func (handler *Handler) UploadMultipartParseInput(r *http.Request) (*s3manager.UploadInput, *multipart.Reader, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
	}
	var reader *multipart.Reader
	logging.Log.Debugf("%v", r.Header)
	if header := r.Header.Get("Content-Type"); header != "" {
		s3Req.ContentType = &header
		mediaType, params, _ := mime.ParseMediaType(*s3Req.ContentType)
		if mediaType == "multipart/related" {
			reader = multipart.NewReader(r.Body, params["boundary"])
			key, err := nameFromMultipart(reader)
			if err != nil {
				return nil, nil, err
			}
			s3Req.Key = &key
			return s3Req, reader, nil
		} else {
			return nil, nil, errors.New("Content type not multipart/related")
		}
	}
	return nil, nil, errors.New("Content type not multipart/related")
}

func nameFromMultipart(reader *multipart.Reader) (string, error) {
	part, err := reader.NextPart()
	if err != nil {
		return "", err
	}
	defer part.Close()

	fileBytes, err := ioutil.ReadAll(part)
	contentType := part.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		return "", errors.New(fmt.Sprintf("First part of multipart is %s not json", contentType))
	}
	jsonMap := make(map[string]interface{})
	if err := json.Unmarshal(fileBytes, &jsonMap); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s", jsonMap["name"]), nil
}

// Handle an upload
func (handler *Handler) UploadMultipartHandle(writer http.ResponseWriter, request *http.Request) {
	s3Req, reader, _ := handler.UploadMultipartParseInput(request)
	logging.Log.Debugf("%v", reader)
	var err error
	var attrs *storage.ObjectAttrs
	part, err := reader.NextPart()
	if err != nil {
		writer.WriteHeader(400)
		logging.Log.Error("Error a %s %s", request.RequestURI, err)
		writer.Write([]byte(string(fmt.Sprint(err))))
		return
	}
	defer part.Close()
	if handler.Config.IsSet("aws_destination_config") {
		logging.LogUsingAWS()
		uploader := s3manager.NewUploaderWithClient(handler.S3Client)
		s3Req.Body = part
		_, err := uploader.Upload(s3Req)
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		attrs = &storage.ObjectAttrs{
			Bucket: *s3Req.Bucket,
			Name: *s3Req.Key,
		}
	} else if handler.Config.IsSet("gcp_destination_config") {
		// Use GCS
		logging.Log.Info("Begin PUT request", request.RequestURI)
		// Log that we are using GCP, get a client based on configurations.  This is from a pool
		client, err := handler.GCPRequestSetup(request)
		if client != nil {
			// return connection to pool after done
			defer handler.ReturnConnection(client, request)
		}
		logging.Log.Infof("Got headers %s %s", *s3Req.Key, *s3Req.ContentType)
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := handler.BucketRename(*s3Req.Bucket)
		bucketHandle := handler.GCPClientToBucket(bucket, client)
		uploader := handler.GCPBucketToObject(*s3Req.Key, bucketHandle).NewWriter(*handler.Context)
		s3Req.Body = part
		_, err = converter.GCPUpload(s3Req, uploader)
		uploaderErr := uploader.Close()
		if err != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, err)
			return
		}
		if uploaderErr != nil {
			writer.WriteHeader(404)
			logging.Log.Error("Error %s %s", request.RequestURI, uploaderErr)
			return
		}
		attrs = uploader.Attrs()
		converter.GCSMD5ToEtag(attrs, writer)
		logging.Log.Info("Finish PUT request", request.RequestURI)
	}
	jsonAttrs, err := json.Marshal(attrs)
	if err != nil {
		writer.WriteHeader(404)
		logging.Log.Error("Error %s %s", request.RequestURI, err)
		return
	}
	logging.Log.Debugf("Upload result %s", jsonAttrs)
	writer.WriteHeader(200)
	writer.Write(jsonAttrs)
}


func New(gcsHandler *gcs_handler.Handler) *Handler {
	return &Handler{gcsHandler, sync.Mutex{}}
}
