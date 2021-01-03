package bucket

import (
	gcs_handler "cloudsidecar/pkg/gcp/handler/gcs"
	"cloudsidecar/pkg/logging"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"google.golang.org/api/storage/v1"
	"net/http"
	"strconv"
)

// Interface for bucket functions
type Bucket interface {
	Register(mux *mux.Router)
	New(s3Handler *gcs_handler.Handler) *Handler
}

type Handler struct {
	*gcs_handler.Handler
}

func New(gcsHandler *gcs_handler.Handler) *Handler {
	return &Handler{gcsHandler}
}


// Register HTTP patterns to functions
func (wrapper *Handler) Register(mux *mux.Router) {
	logging.Log.Infof("Stuff %s", wrapper.Config)
	keyFromUrl := wrapper.Config.Get("gcp_destination_config.key_from_url")
	if keyFromUrl != nil && keyFromUrl == true {
		mux.HandleFunc("/{creds}/storage/v1/b/{bucket}/o", wrapper.ListHandle).Methods("GET")
	} else {
		mux.HandleFunc("/storage/v1/b/{bucket}/o", wrapper.ListHandle).Methods("GET")
	}
}

func (wrapper *Handler) awsToGcpList(output *s3.ListObjectsV2Output) ([]*storage.Object, error) {
	results := make([]*storage.Object, 0)
	for _, obj := range output.Contents {
		results = append(results, &storage.Object{
			Name: *obj.Key,
		})
	}
	for _, obj := range output.CommonPrefixes {
		results = append(results, &storage.Object{
			Name: *obj.Prefix,
		})
	}
	if !*output.IsTruncated {
		return results, nil
	} else {
		req := &s3.ListObjectsV2Input{
			Bucket: output.Name,
			ContinuationToken: output.NextContinuationToken,
			Delimiter: output.Delimiter,
		}
		objects, err := wrapper.S3Client.ListObjectsV2Request(req).Send()
		if err != nil {
			return nil, err
		}
		newResults, err := wrapper.awsToGcpList(objects)
		results = append(results, newResults...)
		return results, err
	}
}

func (wrapper *Handler) ListHandle(writer http.ResponseWriter, request *http.Request) {
	input, err := wrapper.ListParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	if wrapper.Config.IsSet("aws_destination_config") {
		objects, err := wrapper.S3Client.ListObjectsV2Request(input).Send()
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		keys, _ := wrapper.awsToGcpList(objects)
		result := storage.Objects{
			Items: keys,
		}
		js, _ := json.Marshal(result)
		writer.Write(js)
		return

	}
}

func (wrapper *Handler) ListParseInput(request *http.Request) (*s3.ListObjectsV2Input, error) {

	vars := mux.Vars(request)
	bucket := vars["bucket"]
	if bucket == "" {
		return nil, errors.New("no bucket present")
	}
	listRequest := &s3.ListObjectsV2Input{Bucket: &bucket}
	delim := request.URL.Query().Get("delimiter")
	listRequest.Delimiter = &delim
	if encodingType := request.URL.Query().Get("encoding-type"); encodingType == "url" {
		listRequest.EncodingType = s3.EncodingTypeUrl
	}
	if maxKeys := request.URL.Query().Get("max-keys"); maxKeys != "" {
		maxKeyInt, _ := strconv.ParseInt(maxKeys, 10, 64)
		listRequest.MaxKeys = &maxKeyInt
	} else {
		maxKeyInt := int64(1000)
		listRequest.MaxKeys = &maxKeyInt
	}
	prefix := request.URL.Query().Get("prefix")
	listRequest.Prefix = &prefix
	return listRequest, nil
}
