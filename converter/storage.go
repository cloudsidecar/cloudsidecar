package converter

import (
	"cloud.google.com/go/storage"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sidecar/response_type"
)

func gcpPermissionToAWS(role storage.ACLRole) string {
	if role == storage.RoleOwner {
		return string(s3.PermissionFullControl)
	} else if role == storage.RoleReader {
		return string(s3.PermissionRead)
	} else {
		return string(s3.PermissionWrite)
	}
}

func GCSListResponseToAWS(input *storage.ObjectIterator, listRequest *s3.ListObjectsInput) *response_type.AWSListBucketResponse {

	contentI := 0
	prefixI := 0
	fmt.Printf("Remaining %d\n", input.PageInfo().Remaining())
	fmt.Printf("MaxSize %d\n", input.PageInfo().MaxSize)
	fmt.Printf("Token %s\n", input.PageInfo().Token)
	var contents = make([]*response_type.BucketContent, int(*listRequest.MaxKeys))
	var prefixes = make([]*response_type.BucketCommonPrefix, int(*listRequest.MaxKeys))
	for item, err := input.Next() ;  err == nil && input.PageInfo().Remaining() > 0; item, err = input.Next() {
		lastModified := item.Updated
		fmt.Printf("Token %s\n", input.PageInfo().Token)
		fmt.Printf("Remaining %d\n", input.PageInfo().Remaining())
		fmt.Printf("MaxSize %d\n", input.PageInfo().MaxSize)
		if item.Name != "" {
			contents[contentI] = &response_type.BucketContent{
				Key: item.Name,
				LastModified: lastModified.Format("2006-01-02T15:04:05.000Z"),
				ETag: fmt.Sprintf("%x", item.MD5[:]),
				Size: item.Size,
				StorageClass: item.StorageClass,
			}
			contentI++
		} else {
			prefixes[prefixI] = &response_type.BucketCommonPrefix{
				Prefix: item.Prefix,
			}
			prefixI++
		}
	}
	isTruncated := false
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
		ContinuationToken: &input.PageInfo().Token,
	}
	if listRequest.Delimiter != nil && *listRequest.Delimiter != "" {
		s3Resp.Delimiter = listRequest.Delimiter
	}
	return s3Resp
}

func GCSACLResponseToAWS(input []storage.ACLRule) response_type.AWSACLResponse {
	response := response_type.AWSACLResponse{}
	var grants = make([]*response_type.Grant, len(input))
	for i, entry := range input {
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
