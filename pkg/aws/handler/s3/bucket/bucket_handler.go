package bucket

import (
	"cloud.google.com/go/storage"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"net/http"
	s3_handler "cloudsidecar/pkg/aws/handler/s3"
	"cloudsidecar/pkg/converter"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"strconv"
	"strings"
)


type HandlerPlugin interface {
	init(handler s3_handler.Handler)
}

type Handler struct {
	*s3_handler.Handler
}

type Bucket interface {
	ListHandlev2(writer http.ResponseWriter, request *http.Request)
	Listv2ParseInput(r *http.Request) (*s3.ListObjectsInput, error)
	ListHandle(writer http.ResponseWriter, request *http.Request)
	ListParseInput(r *http.Request) (*s3.ListObjectsInput, error)
	ACLHandle(writer http.ResponseWriter, request *http.Request)
	ACLParseInput(r *http.Request) (*s3.GetBucketAclInput, error)
	Register(mux *mux.Router)
	New(s3Handler *s3_handler.Handler) *Handler
}

func New(s3Handler *s3_handler.Handler) *Handler {
	return &Handler{s3Handler}
}

func (wrapper *Handler) Register(mux *mux.Router) {
	keyFromUrl := wrapper.Config.Get("gcp_destination_config.key_from_url")
	if keyFromUrl != nil && keyFromUrl == true{
		mux.HandleFunc("/{creds}/{bucket}", wrapper.ACLHandle).Queries("acl", "").Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}/", wrapper.ACLHandle).Queries("acl", "").Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}", wrapper.ListHandlev2).Queries("list-type", "2").Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}/", wrapper.ListHandlev2).Queries("list-type", "2").Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}", wrapper.ListHandle).Methods("GET")
		mux.HandleFunc("/{creds}/{bucket}/", wrapper.ListHandle).Methods("GET")
	} else {
		mux.HandleFunc("/{bucket}", wrapper.ACLHandle).Queries("acl", "").Methods("GET")
		mux.HandleFunc("/{bucket}/", wrapper.ACLHandle).Queries("acl", "").Methods("GET")
		mux.HandleFunc("/{bucket}", wrapper.ListHandlev2).Queries("list-type", "2").Methods("GET")
		mux.HandleFunc("/{bucket}/", wrapper.ListHandlev2).Queries("list-type", "2").Methods("GET")
		mux.HandleFunc("/{bucket}", wrapper.ListHandle).Methods("GET")
		mux.HandleFunc("/{bucket}/", wrapper.ListHandle).Methods("GET")
	}
}

func (wrapper *Handler) ListParseInput(request *http.Request) (*s3.ListObjectsInput, error) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	if bucket == "" {
		return nil, errors.New("no bucket present")
	}
	listRequest := &s3.ListObjectsInput{Bucket: &bucket}
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
	if marker := request.URL.Query().Get("marker"); marker != "" {
		listRequest.Marker = &marker
	}
	prefix := request.URL.Query().Get("prefix")
	listRequest.Prefix = &prefix
	return listRequest, nil
}

func (wrapper *Handler) ListHandle(writer http.ResponseWriter, request *http.Request) {
	input, err := wrapper.ListParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	bucket := *input.Bucket
	var response *response_type.AWSListBucketResponse
	pageSize := 1000
	if input.MaxKeys != nil {
		pageSize = int(*input.MaxKeys)
	}
	if wrapper.GCPClient != nil {
		client := wrapper.GCPRequestSetup(request)
		bucket = wrapper.BucketRename(bucket)
		bucketObject := wrapper.GCPClientToBucket(bucket, client)
		it := bucketObject.Objects(*wrapper.Context, &storage.Query{
			Delimiter: *input.Delimiter,
			Prefix: *input.Prefix,
			Versions: false,
		})
		response, err = converter.GCSListResponseToAWS(it, input, pageSize)
		if err != nil {
			logging.Log.Error("Error %s %s\n", request.RequestURI, err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	} else {
		logging.LogUsingAWS()
		req := wrapper.S3Client.ListObjectsRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			logging.Log.Error("Error %s %s\n", request.RequestURI, respError)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
		}
		var contents= make([]*response_type.BucketContent, len(resp.Contents))
		for i, content := range resp.Contents {
			contents[i] = &response_type.BucketContent{
				Key:          *content.Key,
				LastModified: content.LastModified.Format("2006-01-02T15:04:05.000Z"),
				ETag:         *content.ETag,
				Size:         *content.Size,
				StorageClass: string(content.StorageClass),
			}
		}
		var prefixes= make([]*response_type.BucketCommonPrefix, len(resp.CommonPrefixes))
		for i, prefix := range resp.CommonPrefixes {
			prefixes[i] = &response_type.BucketCommonPrefix{
				Prefix: *prefix.Prefix,
			}
		}
		response = &response_type.AWSListBucketResponse{
			XmlNS:                 "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:                  resp.Name,
			Prefix:                resp.Prefix,
			Delimiter:             nil,
			Marker:                resp.Marker,
			KeyCount:              int64(len(contents)),
			MaxKeys:               resp.MaxKeys,
			IsTruncated:           resp.IsTruncated,
			Contents:              contents,
			CommonPrefixes:        prefixes,
			NextContinuationToken: resp.NextMarker,
		}
		if resp.Delimiter != nil && *resp.Delimiter != "" {
			response.Delimiter = resp.Delimiter
		}
	}
	output, _ := xml.Marshal(response)
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (wrapper *Handler) Listv2ParseInput(request *http.Request) (*s3.ListObjectsInput, error) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	if bucket == "" {
		return nil, errors.New("no bucket present")
	}
	listRequest := &s3.ListObjectsInput{Bucket: &bucket}
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
	if continuationToken := request.URL.Query().Get("continuation-token"); continuationToken != "" {
		listRequest.Marker = &continuationToken
	}
	prefix := request.URL.Query().Get("prefix")
	listRequest.Prefix = &prefix
	if startAfter := request.URL.Query().Get("start-after"); startAfter != "" {
		listRequest.Marker = &startAfter
	}
	return listRequest, nil
}

func (wrapper *Handler) ListHandlev2(writer http.ResponseWriter, request *http.Request) {
	input, err := wrapper.Listv2ParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	bucket := *input.Bucket
	var response *response_type.AWSListBucketResponse
	pageSize := 1000
	if input.MaxKeys != nil {
		pageSize = int(*input.MaxKeys)
	}
	if wrapper.GCPClient != nil {
		client := wrapper.GCPRequestSetup(request)
		bucket = wrapper.BucketRename(bucket)
		bucketObject := wrapper.GCPClientToBucket(bucket, client)
		it := bucketObject.Objects(*wrapper.Context, &storage.Query{
			Delimiter: *input.Delimiter,
			Prefix: *input.Prefix,
			Versions: false,
		})
		response, err = converter.GCSListResponseToAWSv2(it, input, pageSize)
		if err != nil {
			logging.Log.Error("Error %s %s\n", request.RequestURI, err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	} else {
		logging.LogUsingAWS()
		req := wrapper.S3Client.ListObjectsRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			logging.Log.Error("Error %s %s\n", request.RequestURI, respError)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
		}
		var contents= make([]*response_type.BucketContent, len(resp.Contents))
		for i, content := range resp.Contents {
			contents[i] = &response_type.BucketContent{
				Key:          *content.Key,
				LastModified: content.LastModified.Format("2006-01-02T15:04:05.000Z"),
				ETag:         *content.ETag,
				Size:         *content.Size,
				StorageClass: string(content.StorageClass),
			}
		}
		var prefixes= make([]*response_type.BucketCommonPrefix, len(resp.CommonPrefixes))
		for i, prefix := range resp.CommonPrefixes {
			prefixes[i] = &response_type.BucketCommonPrefix{
				Prefix: *prefix.Prefix,
			}
		}
		response = &response_type.AWSListBucketResponse{
			XmlNS:                 "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:                  resp.Name,
			Prefix:                resp.Prefix,
			Delimiter:             nil,
			Marker:                resp.Marker,
			KeyCount:              int64(len(contents)),
			MaxKeys:               resp.MaxKeys,
			IsTruncated:           resp.IsTruncated,
			Contents:              contents,
			CommonPrefixes:        prefixes,
			NextContinuationToken: resp.NextMarker,
		}
		if resp.Delimiter != nil && *resp.Delimiter != "" {
			response.Delimiter = resp.Delimiter
		}
	}
	output, _ := xml.Marshal(response)
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}


func (wrapper *Handler) ACLParseInput(r *http.Request) (*s3.GetBucketAclInput, error) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	return &s3.GetBucketAclInput{Bucket: &bucket}, nil
}

func (wrapper *Handler) ACLHandle(writer http.ResponseWriter, request *http.Request) {
	input, _ := wrapper.ACLParseInput(request)
	if wrapper.GCPClient != nil {
		client := wrapper.GCPRequestSetup(request)
		bucket := wrapper.BucketRename(*input.Bucket)
		acl := wrapper.GCPClientToBucket(bucket, client).ACL()
		aclList, err := acl.List(*wrapper.Context)
		if err != nil {
			logging.Log.Error("Error with GCP %s %s", request.RequestURI, err)
			if strings.Contains(err.Error(), "Error 403") {
				code := "AccessDenied"
				message := "Access Denied"
				xmlResponse := response_type.AWSACLResponseError{
					Code: &code,
					Message: &message,
				}
				writer.WriteHeader(403)
				output, _ := xml.MarshalIndent(xmlResponse, "  ", "    ")
				writer.Write([]byte(s3_handler.XmlHeader))
				writer.Write([]byte(string(output)))
			} else {
				writer.WriteHeader(404)
			}
			return
		}
		output, _ := xml.MarshalIndent(converter.GCSACLResponseToAWS(aclList), "  ", "    ")
		writer.Write([]byte(s3_handler.XmlHeader))
		writer.Write([]byte(string(output)))
	} else {
		logging.LogUsingAWS()
		req := wrapper.S3Client.GetBucketAclRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			panic(fmt.Sprintf("Error %s", respError))
		}
		var grants = make([]*response_type.Grant, len(resp.Grants))
		for i, grant := range resp.Grants {
			grants[i] = &response_type.Grant{
				Grantee: &response_type.Grantee{
					Id: *grant.Grantee.ID,
					DisplayName: *grant.Grantee.DisplayName,
					XmlNS: response_type.ACLXmlNs,
					Xsi: response_type.ACLXmlXsi,
				},
				Permission: string(grant.Permission),
			}
		}
		s3Resp := &response_type.AWSACLResponse{
			OwnerId: *resp.Owner.ID,
			OwnerDisplayName: *resp.Owner.DisplayName,
			AccessControlList: &response_type.AccessControlList{
				Grants: grants,
			},
		}
		output, _ := xml.MarshalIndent(s3Resp, "  ", "    ")
		writer.Write([]byte(s3_handler.XmlHeader))
		writer.Write([]byte(string(output)))
	}
}
