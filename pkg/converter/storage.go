package converter

import (
	"cloud.google.com/go/storage"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"google.golang.org/api/iterator"
	"io"
	"net/http"
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
	lastModified := FormatTimeZulu(&item.Updated)
	other := MD5toEtag(item.MD5)
	return &response_type.BucketContent{
		Key: item.Name,
		LastModified: lastModified,
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

// Old version of aws listing does not paginate nicely.  It uses the last item of the prior list to
// do an offset.  So here we will just paginate through till we find that item.
func GCSListResponseToAWS(input *storage.ObjectIterator, listRequest *s3.ListObjectsInput, pageSize int) (*response_type.AWSListBucketResponse, error) {
	contentI := 0
	prefixI := 0
	var marker string
	if listRequest.Marker != nil && *listRequest.Marker != "" {
		marker = *listRequest.Marker
	}
	var contents = make([]*response_type.BucketContent, pageSize)
	var prefixes = make([]*response_type.BucketCommonPrefix, pageSize)
	nextToken := ""
	var err error
	lastItem := ""
	for contentI + prefixI < pageSize {
		var pageResponse []*storage.ObjectAttrs
		nextToken, err = iterator.NewPager(input, pageSize, nextToken).NextPage(&pageResponse)
		if err != nil{
			logging.Log.Error("Some error paginating", err)
			return nil, err
		}
		for _, item := range pageResponse {
			if contentI + prefixI >= pageSize {
				break
			}
			if marker != "" {
				if item.Name == marker || item.Prefix == marker {
					marker = ""
				}
			} else {
				if strings.HasSuffix(item.Name, "/") {

				} else if item.Name != "" {
					contents[contentI] = GCSItemToContent(item)
					lastItem = item.Name
					contentI++
				} else {
					prefixes[prefixI] = GCSItemToPrefix(item)
					lastItem = item.Prefix
					prefixI++
				}
			}
		}
		if len(pageResponse) == 0 || nextToken == "" {
			break
		}
	}
	if nextToken != "" {
		nextToken = lastItem
	}
	contents = contents[:contentI]
	prefixes = prefixes[:prefixI]
	s3Resp := GCSListResponseObjectsToAWS(contents, listRequest, nextToken, contentI, prefixI, prefixes)
	return s3Resp, nil
}

func GCSListResponseToAWSv2(input *storage.ObjectIterator, listRequest *s3.ListObjectsInput, pageSize int) (*response_type.AWSListBucketResponse, error) {
	contentI := 0
	prefixI := 0
	var pageResponse []*storage.ObjectAttrs
	var marker string
	if listRequest.Marker != nil && *listRequest.Marker != "" {
		marker = *listRequest.Marker
	}
	nextToken, err := iterator.NewPager(input, pageSize, marker).NextPage(&pageResponse)
	if err != nil{
		return nil, err
	}
	var contents = make([]*response_type.BucketContent, len(pageResponse))
	var prefixes = make([]*response_type.BucketCommonPrefix, len(pageResponse))
	for _, item := range pageResponse {
		if strings.HasSuffix(item.Name, "/") {

		} else if item.Name != "" {
			contents[contentI] = GCSItemToContent(item)
			contentI++
		} else {
			prefixes[prefixI] = GCSItemToPrefix(item)
			prefixI++
		}
	}
	s3Resp := GCSListResponseObjectsToAWS(contents, listRequest, nextToken, contentI, prefixI, prefixes)
	return s3Resp, nil
}

func GCSAttrToCombine(input *storage.ObjectAttrs) *response_type.CompleteMultipartUploadResult {
	etag := MD5toEtag(input.MD5)
	location := fmt.Sprintf("http://%s.s3.amazonaws.com/%s", input.Bucket, input.Name)
	return &response_type.CompleteMultipartUploadResult{
		Bucket: &input.Bucket,
		Key: &input.Name,
		ETag: &etag,
		Location: &location,
	}
}

func GCSAttrToHeaders(input *storage.ObjectAttrs, writer http.ResponseWriter) {
	utc, _ := time.LoadLocation("UTC")
	writer.Header().Set("Content-Length", strconv.FormatInt(input.Size, 10))
	if input.CacheControl != "" {
		writer.Header().Set("Cache-Control", input.CacheControl)
	}
	if input.ContentType != "" {
		writer.Header().Set("Cache-Type", input.ContentType)
	}
	if len(input.MD5) > 0 {
		other := MD5toEtag(input.MD5)
		writer.Header().Set("ETag", other)
	}
	lastMod := input.Updated.In(utc).Format(time.RFC1123)
	lastMod = strings.Replace(lastMod, "UTC", "GMT", 1)
	writer.Header().Set("Last-Modified", lastMod)
}

func GCSACLResponseToAWS(input []storage.ACLRule) response_type.AWSACLResponse {
	response := response_type.AWSACLResponse{}
	// lets only do head
	// var grants = make([]*response_type.Grant, len(input))
	var grants = make([]*response_type.Grant, 1)
	if len(input) > 0 {
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
	} else {
		response.AccessControlList = &response_type.AccessControlList{
		}
	}
	return response
}

func MD5toEtag(input []byte) string {
	return fmt.Sprintf("%x", input)
	//apparently GCP etag is a base64 encoding, aws is just md5
	//return base64.StdEncoding.EncodeToString(input)
}

func FormatTimeZulu(input *time.Time) string {
	utc, _ := time.LoadLocation("UTC")
	return input.In(utc).Format("2006-01-02T15:04:05.000Z")
}

func GCSCopyResponseToAWS(input *storage.ObjectAttrs) response_type.CopyResult {
	return response_type.CopyResult{
		LastModified: FormatTimeZulu(&input.Updated),
		ETag:         MD5toEtag(input.MD5),
	}
}
