package bucket

import (
	original_storage "cloud.google.com/go/storage"
	gcs_handler "cloudsidecar/pkg/gcp/handler/gcs"
	"cloudsidecar/pkg/logging"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
	"google.golang.org/api/storage/v1"
	"net/http"
	"strconv"
)

// Interface for bucket functions
type Bucket interface {
	Register(mux *mux.Router)
	New(s3Handler *gcs_handler.Handler) *Handler
	ListHandle(writer http.ResponseWriter, request *http.Request)
	ListParseInput(request *http.Request) (*s3.ListObjectsV2Input, error)
	ACLHandle(writer http.ResponseWriter, request *http.Request)
	ACLParseInput(r *http.Request) (*s3.GetBucketAclInput, error)
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
		mux.HandleFunc("/{creds}/storage/v1/b/{bucket}/acl", wrapper.ACLHandle).Methods("GET")
	} else {
		mux.HandleFunc("/storage/v1/b/{bucket}/o", wrapper.ListHandle).Methods("GET")
		mux.HandleFunc("/storage/v1/b/{bucket}/acl", wrapper.ACLHandle).Methods("GET")
	}
}

func (wrapper *Handler) ACLHandle(writer http.ResponseWriter, request *http.Request) {
	input, err := wrapper.ACLParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	if wrapper.Config.IsSet("aws_destination_config") {
		logging.LogUsingAWS()
		resp, respError := wrapper.S3Client.GetBucketAclRequest(input).Send()
		if respError != nil {
			panic(fmt.Sprintf("Error %s", respError))
		}
		bucket := wrapper.BucketRename(*input.Bucket)
		input.Bucket = &bucket
		grants := make([]*storage.BucketAccessControl, len(resp.Grants))
		for i, grant := range resp.Grants {
			perm, _ := grant.Permission.MarshalValue()
			var displayName string
			if perm == "FULL_CONTROL" {
				displayName = fmt.Sprintf("project-owners-%s", *grant.Grantee.DisplayName)
			}
			grants[i] = &storage.BucketAccessControl{
				Bucket: *input.Bucket,
				Role:   perm,
				Entity: displayName,
			}
		}
		result := storage.BucketAccessControls{
			Items: grants,
		}
		js, _ := json.Marshal(result)
		writer.Write(js)
		return
	} else if wrapper.Config.IsSet("gcp_destination_config") {
		client, err := wrapper.GCPRequestSetup(request)
		if client != nil {
			// Return connection to pool after done
			defer wrapper.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := *input.Bucket
		bucketObject := wrapper.GCPClientToBucket(bucket, client)
		acl := bucketObject.ACL()
		rules, err := acl.List(*wrapper.Context)
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		acls := make([]*storage.BucketAccessControl, len(rules))
		for i, rule := range rules {
			acls[i] = &storage.BucketAccessControl{
				Bucket: bucket,
				Role:   string(rule.Role),
				Entity: string(rule.Entity),
			}
		}
		result := storage.BucketAccessControls{
			Items: acls,
		}
		js, _ := json.Marshal(result)
		writer.Write(js)
		return
	}
}

func (wrapper *Handler) ACLParseInput(r *http.Request) (*s3.GetBucketAclInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	return &s3.GetBucketAclInput{Bucket: &bucket}, nil
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
			Bucket:            output.Name,
			ContinuationToken: output.NextContinuationToken,
			Delimiter:         output.Delimiter,
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
		logging.LogUsingAWS()
		bucket := wrapper.BucketRename(*input.Bucket)
		input.Bucket = &bucket
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
	} else if wrapper.Config.IsSet("gcp_destination_config") {
		client, err := wrapper.GCPRequestSetup(request)
		if client != nil {
			// Return connection to pool after done
			defer wrapper.ReturnConnection(client, request)
		}
		if err != nil {
			writer.WriteHeader(400)
			logging.Log.Error("Error a %s %s", request.RequestURI, err)
			writer.Write([]byte(string(fmt.Sprint(err))))
			return
		}
		bucket := *input.Bucket
		bucketObject := wrapper.GCPClientToBucket(bucket, client)
		it := bucketObject.Objects(*wrapper.Context, &original_storage.Query{
			Delimiter: *input.Delimiter,
			Prefix:    *input.Prefix,
			Versions:  false,
		})
		keys := make([]*storage.Object, 0)
		for {
			row, err := it.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				writer.WriteHeader(400)
				logging.Log.Error("Error a %s %s", request.RequestURI, err)
				writer.Write([]byte(string(fmt.Sprint(err))))
				return
			}
			keys = append(keys, &storage.Object{
				Bucket: bucket,
				Name:   row.Name,
			})
		}
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
