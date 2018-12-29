package handler

import (
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"
)

type S3Handler struct {
	S3Client *s3.S3
}

type AWSACLResponse struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	OwnerId string `xml:"Owner>ID"`
	OwnerDisplayName string `xml:"Owner>DisplayName"`
	AccessControlList *AccessControlList `xml:"AccessControlList"`
}

type AccessControlList struct {
	Grants []*Grant `xml:"Grant"`
}

type Grant struct {
	Grantee *Grantee `xml:"Grantee"`
	Permission string `xml:"Permission"`
}

type Grantee struct {
	XMLName xml.Name `xml:"Grantee"`
	Id string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
	XmlNS string `xml:"xmlns:xsi,attr"`
	Xsi string `xml:"xsi:type,attr"`
}

type AWSListBucketResponse struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	XmlNS string `xml:"xmlns,attr"`
	Name *string `xml:"Name"`
	Prefix *string `xml:"Prefix"`
	Delimiter *string `xml:"Delimiter,omitempty"`
	StartAfter *string `xml:"StartAfter,omitempty"`
	KeyCount int64 `xml:"KeyCount"`
	MaxKeys *int64 `xml:"MaxKeys"`
	IsTruncated *bool `xml:"IsTruncated"`
	Contents []*BucketContent `xml:"Contents"`
	CommonPrefixes []*BucketCommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type BucketContent struct {
	Key string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag string `xml:"ETag"`
	Size int64 `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type BucketCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

const (
	xmlHeader string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
)

func write(input string, writer *http.ResponseWriter) {
	line := fmt.Sprintf("%s", input)
	_, err := (*writer).Write([]byte(line))
	if err != nil {
		panic(fmt.Sprintf("Error %s", err))
	}
}

func writeLine(input string, writer *http.ResponseWriter) {
	line := fmt.Sprintf("%s\n", input)
	_, err := (*writer).Write([]byte(line))
	if err != nil {
		panic(fmt.Sprintf("Error %s", err))
	}
}

func (handler S3Handler) S3ACL(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	req := handler.S3Client.GetBucketAclRequest(&s3.GetBucketAclInput{Bucket: &bucket})
	resp, respError := req.Send()
	if respError != nil {
		panic(fmt.Sprintf("Error %s", respError))
	}
	var grants = make([]*Grant, len(resp.Grants))
	for i, grant := range resp.Grants {
		grants[i] = &Grant{
			Grantee: &Grantee{
				Id: *grant.Grantee.ID,
				DisplayName: *grant.Grantee.DisplayName,
				XmlNS: "http://www.w3.org/2001/XMLSchema-instance",
				Xsi: "CanonicalUser",
			},
			Permission: string(grant.Permission),
		}
	}
	s3Resp := &AWSACLResponse{
		OwnerId: *resp.Owner.ID,
		OwnerDisplayName: *resp.Owner.DisplayName,
		AccessControlList: &AccessControlList{
			Grants: grants,
		},
	}
	output, _ := xml.MarshalIndent(s3Resp, "  ", "    ")
	fmt.Printf("Response %s", resp)
	writeLine(xmlHeader, &writer)
	writeLine(string(output), &writer)
	return
}

func (handler S3Handler) S3HeadFile(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	key := vars["key"]
	fmt.Printf("Looking for %s %s\n", bucket, key)
	fmt.Printf("URL %s\n", request.URL)
	req := handler.S3Client.HeadObjectRequest(&s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	resp, respError := req.Send()
	if respError != nil {
		writer.WriteHeader(404)
		fmt.Printf("Error %s", respError)
		return
	}
	fmt.Printf("Response %s\n", resp.String())
	if resp.AcceptRanges != nil {
		writer.Header().Set("Accept-Ranges", *resp.AcceptRanges)
	}
	if resp.ContentLength != nil {
		writer.Header().Set("Content-Length", strconv.FormatInt(*resp.ContentLength, 10))
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
		writer.Header().Set("Last-Modified", resp.LastModified.Format(time.RFC1123Z))
	}
	writer.WriteHeader(200)
	return
}

func (handler S3Handler) S3List(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	fmt.Printf("Headers: %s", request.URL)
	listRequest := &s3.ListObjectsInput{Bucket: &bucket}
	delim := request.URL.Query().Get("delimiter")
	listRequest.Delimiter = &delim
	if encodingType := request.URL.Query().Get("encoding-type"); encodingType == "url" {
		listRequest.EncodingType = s3.EncodingTypeUrl
	}
	if maxKeys := request.URL.Query().Get("max-keys"); maxKeys != "" {
		maxKeyInt, _ := strconv.ParseInt(maxKeys, 10, 64)
		listRequest.MaxKeys = &maxKeyInt
	}
	prefix := request.URL.Query().Get("prefix")
	listRequest.Prefix = &prefix
	if startAfter := request.Header.Get("start-after"); startAfter != "" {
		listRequest.Marker = &startAfter
	}
	fmt.Printf("Requesting %s", listRequest)
	req := handler.S3Client.ListObjectsRequest(listRequest)
	resp, respError := req.Send()
	if respError != nil {
		panic(fmt.Sprintf("Error %s", respError))
	}
	var contents = make([]*BucketContent, len(resp.Contents))
	for i, content := range resp.Contents {
		contents[i] = &BucketContent{
			Key: *content.Key,
			LastModified: content.LastModified.Format(time.RFC3339),
			ETag: *content.ETag,
			Size: *content.Size,
			StorageClass: string(content.StorageClass),
		}
	}
	var prefixes = make([]*BucketCommonPrefix, len(resp.CommonPrefixes))
	for i, prefix := range resp.CommonPrefixes {
		prefixes[i] = &BucketCommonPrefix{
			*prefix.Prefix,
		}
	}
	s3Resp := &AWSListBucketResponse{
		XmlNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		Name: resp.Name,
		Prefix: resp.Prefix,
		Delimiter: nil,
		StartAfter: nil,
		KeyCount: int64(len(contents)),
		MaxKeys: resp.MaxKeys,
		IsTruncated: resp.IsTruncated,
		Contents: contents,
		CommonPrefixes: prefixes,
	}
	if resp.Delimiter != nil && *resp.Delimiter != "" {
		s3Resp.Delimiter = resp.Delimiter
	}
	if resp.Marker != nil && *resp.Marker != "" {
		s3Resp.StartAfter = resp.Marker
	}
	output, _ := xml.Marshal(s3Resp)
	fmt.Printf("Response %s", resp)
	writeLine(xmlHeader, &writer)
	write(string(output), &writer)
	return
}