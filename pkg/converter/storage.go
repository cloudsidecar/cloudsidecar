package converter

import (
	"cloud.google.com/go/storage"
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"google.golang.org/api/iterator"
	"io"
	"net/http"
	"sidecar/pkg/logging"
	"sidecar/pkg/response_type"
	"strconv"
	"strings"
	"time"
)

type Writer interface {
	Write(p []byte) (n int, err error)
}

func gcpPermissionToAWS(role storage.ACLRole) string {
	if role == storage.RoleOwner {
		return string(s3.PermissionFullControl)
	} else if role == storage.RoleReader {
		return string(s3.PermissionRead)
	} else {
		return string(s3.PermissionWrite)
	}
}

func GCPUpload(input *s3manager.UploadInput, writer Writer) (int64, error) {
	reader := input.Body
	buffer := make([]byte, 4096)
	var bytes int64
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			bytes += int64(n)
			logging.Log.Debug("Read bytes ", n)
			_, writeErr := writer.Write(buffer[:n])
			if writeErr != nil {
				logging.Log.Error("Write error ", writeErr)
				return bytes, writeErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			logging.Log.Error("ERROR ", err)
			return bytes, err
		}
	}
	return bytes, nil
}

func GCSListResponseObjectsToAWS(contents []*response_type.BucketContent, listRequest *s3.ListObjectsInput, nextToken string, contentI int, prefixI int, prefixes []*response_type.BucketCommonPrefix) *response_type.AWSListBucketResponse{
	isTruncated := nextToken != ""
	s3Resp := &response_type.AWSListBucketResponse{
		XmlNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		Name: listRequest.Bucket,
		Prefix: listRequest.Prefix,
		Delimiter: nil,
		KeyCount: int64(contentI + prefixI),
		MaxKeys: listRequest.MaxKeys,
		IsTruncated: &isTruncated,
		Contents: contents,
		CommonPrefixes: prefixes,
		ContinuationToken: listRequest.Marker,
		NextContinuationToken: &nextToken,
	}
	if listRequest.Delimiter != nil && *listRequest.Delimiter != "" {
		s3Resp.Delimiter = listRequest.Delimiter
	}
	return s3Resp
}

func GCSItemToContent(item *storage.ObjectAttrs) *response_type.BucketContent {
	utc, _ := time.LoadLocation("UTC")
	lastModified := item.Updated.In(utc)
	other := base64.StdEncoding.EncodeToString(item.MD5)
	return &response_type.BucketContent{
		Key: item.Name,
		LastModified: lastModified.Format("2006-01-02T15:04:05.000Z"),
		// ETag: fmt.Sprintf("%x", item.MD5[:]),
		ETag: other,
		Size: item.Size,
		StorageClass: item.StorageClass,
	}
}

func GCSItemToPrefix(item *storage.ObjectAttrs) *response_type.BucketCommonPrefix {
	return &response_type.BucketCommonPrefix{
		Prefix: item.Prefix,
	}
}

func GCSListResponseToAWS(input *storage.ObjectIterator, listRequest *s3.ListObjectsInput) *response_type.AWSListBucketResponse {
	contentI := 0
	prefixI := 0
	var pageResponse []*storage.ObjectAttrs
	var marker string
	if listRequest.Marker != nil && *listRequest.Marker != "" {
		marker = *listRequest.Marker
	}
	nextToken, err := iterator.NewPager(input, 1000, marker).NextPage(&pageResponse)
	if err != nil{
		panic(fmt.Sprintf("Boo %s", err))
	}
	var contents = make([]*response_type.BucketContent, len(pageResponse))
	var prefixes = make([]*response_type.BucketCommonPrefix, len(pageResponse))
	for _, item := range pageResponse {
		if item.Name != "" {
			contents[contentI] = GCSItemToContent(item)
			contentI++
		} else {
			prefixes[prefixI] = GCSItemToPrefix(item)
			prefixI++
		}
	}
	s3Resp := GCSListResponseObjectsToAWS(contents, listRequest, nextToken, contentI, prefixI, prefixes)
	return s3Resp
}

func GCSAttrToCombine(input *storage.ObjectAttrs) *response_type.CompleteMultipartUploadResult {
	etag := base64.StdEncoding.EncodeToString(input.MD5)
	location := fmt.Sprintf("http://%s.s3.amazonaws.com/%s", input.Bucket, input.Name)
	return &response_type.CompleteMultipartUploadResult{
		Bucket: &input.Bucket,
		Key: &input.Name,
		ETag: &etag,
		Location: &location,
	}
}

func GCSAttrToHeaders(input *storage.ObjectAttrs, writer http.ResponseWriter) {
	writer.Header().Set("Content-Length", strconv.FormatInt(input.Size, 10))
	if input.CacheControl != "" {
		writer.Header().Set("Cache-Control", input.CacheControl)
	}
	if input.ContentType != "" {
		writer.Header().Set("Cache-Type", input.ContentType)
	}
	if len(input.MD5) > 0 {
		other := base64.StdEncoding.EncodeToString(input.MD5)
		writer.Header().Set("ETag", other)
	}
	lastMod := input.Updated.Format(time.RFC1123)
	lastMod = strings.Replace(lastMod, "UTC", "GMT", 1)
	writer.Header().Set("Last-Modified", lastMod)
}

func GCSACLResponseToAWS(input []storage.ACLRule) response_type.AWSACLResponse {
	response := response_type.AWSACLResponse{}
	// lets only do head
	// var grants = make([]*response_type.Grant, len(input))
	var grants = make([]*response_type.Grant, 1)
	for i, entry := range input[:1] {
		var displayName string
		if entry.Email != "" {
			displayName = entry.Email
		} else {
			displayName = string(entry.Entity)
		}
		if entry.Role == storage.RoleOwner && response.OwnerId == "" {
			response.OwnerId = string(entry.Entity)
			response.OwnerDisplayName = displayName
		}
		grant := &response_type.Grant{
			Permission: gcpPermissionToAWS(entry.Role),
			Grantee: &response_type.Grantee{
				DisplayName: displayName,
				Id: string(entry.Entity),
				XmlNS: response_type.ACLXmlNs,
				Xsi: response_type.ACLXmlXsi,
			},
		}
		grants[i] = grant
	}
	response.AccessControlList = &response_type.AccessControlList{
		Grants: grants,
	}
	return response
}
