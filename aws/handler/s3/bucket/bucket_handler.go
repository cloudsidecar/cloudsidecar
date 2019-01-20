package bucket

import (
	"cloud.google.com/go/storage"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"net/http"
	s3_handler "sidecar/aws/handler/s3"
	"sidecar/converter"
	"sidecar/response_type"
	"strconv"
)


type HandlerPlugin interface {
	init(handler s3_handler.Handler)
}

type Handler struct {
	s3_handler.Handler
}


type Bucket interface {
	ListHandle(writer http.ResponseWriter, request *http.Request)
	ListParseInput(r *http.Request) (*s3.ListObjectsInput, error)
	ACLHandle(writer http.ResponseWriter, request *http.Request)
	ACLParseInput(r *http.Request) (*s3.ListObjectsInput, error)
}


func (wrapper *Handler) bucketRename(bucket string) string {
	fmt.Println(wrapper, wrapper.Config)
	renameMap := wrapper.Config.GetStringMapString("gcp_destination_config.gcs_config.bucket_rename")
	if renameMap != nil {
		if val, ok := renameMap[bucket]; ok {
			return val
		}
	}
	return bucket
}

func (wrapper Handler) ListParseInput(request *http.Request) (*s3.ListObjectsInput, error) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	fmt.Printf("Headers: %s\n", request.URL)
	listRequest := &s3.ListObjectsInput{Bucket: &bucket}
	delim := request.URL.Query().Get("delimiter")
	listRequest.Delimiter = &delim
	if encodingType := request.URL.Query().Get("encoding-response_type"); encodingType == "url" {
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

func (wrapper *Handler) ListHandle(writer http.ResponseWriter, request *http.Request) {
	input, err := wrapper.ListParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
	}
	bucket := *input.Bucket
	var response *response_type.AWSListBucketResponse
	if wrapper.GCPClient != nil {
		bucket = wrapper.bucketRename(bucket)
		bucketObject := wrapper.GCPClient.Bucket(bucket)
		it := bucketObject.Objects(*wrapper.Context, &storage.Query{
			Delimiter: *input.Delimiter,
			Prefix: *input.Prefix,
			Versions: false,
		})
		response = converter.GCSListResponseToAWS(it, input)
	} else {
		fmt.Printf("Requesting %s", request)
		req := wrapper.S3Client.ListObjectsRequest(input)
		resp, respError := req.Send()
		if respError != nil {
			fmt.Printf("Error %s\n", respError)
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
	fmt.Printf("Response %s", response)
	writer.Write([]byte(s3_handler.XmlHeader))
	writer.Write([]byte(string(output)))
}


func (wrapper *Handler) S3ACL(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	if wrapper.GCPClient != nil {
		bucket = wrapper.bucketRename(bucket)
		acl := wrapper.GCPClient.Bucket(bucket).ACL()
		aclList, err := acl.List(*wrapper.Context)
		if err != nil {
			fmt.Printf("Error with GCP %s", err)
			writer.WriteHeader(404)
			return
		}
		output, _ := xml.MarshalIndent(converter.GCSACLResponseToAWS(aclList), "  ", "    ")
		fmt.Printf("Response %s", aclList)
		writer.Write([]byte(s3_handler.XmlHeader))
		writer.Write([]byte(string(output)))
	} else {
		req := wrapper.S3Client.GetBucketAclRequest(&s3.GetBucketAclInput{Bucket: &bucket})
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
		fmt.Printf("Response %s", resp)
		writer.Write([]byte(s3_handler.XmlHeader))
		writer.Write([]byte(string(output)))
	}
}
