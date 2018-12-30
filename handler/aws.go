package handler

import (
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"strconv"
	"strings"
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
	Marker *string `xml:"Marker"`
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

func writeBytes(input []byte, writer *http.ResponseWriter) {
	_, err := (*writer).Write(input)
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

func (handler S3Handler) S3PutFile(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3manager.UploadInput{
		Bucket: &bucket,
		Key: &key,

	}
	if header := request.Header.Get("Content-MD5"); header != "" {
		s3Req.ContentMD5 = &header
	}
	if header := request.Header.Get("Content-Type"); header != "" {
		s3Req.ContentType = &header
	}
	defer request.Body.Close()
	uploader := s3manager.NewUploaderWithClient(handler.S3Client)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: &bucket,
		Key: &key,
		Body: request.Body,
	})
	if err != nil {
		fmt.Printf("Error %s", err)
	}
	writer.WriteHeader(200)
	fmt.Printf("DONE")
	write("", &writer)
	return
}

func (handler S3Handler) S3GetFile(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	key := vars["key"]
	s3Req := &s3.GetObjectInput{Bucket: &bucket, Key: &key}
	if header := request.Header.Get("Range"); header != "" {
		s3Req.Range = &header
	}
	req := handler.S3Client.GetObjectRequest(&s3.GetObjectInput{Bucket: &bucket, Key: &key})
	resp, respError := req.Send()
	if respError != nil {
		writer.WriteHeader(404)
		fmt.Printf("Error %s", respError)
		return
	}
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
	defer resp.Body.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buffer)
		if n > 0 {
			writeBytes(buffer[:n], &writer)
		}
		if err == io.EOF {
			break
		}
	}
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
	if resp.ServerSideEncryption != "" {
		writer.Header().Set("x-amz-server-side-encryption", string(resp.ServerSideEncryption))
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
		lastMod := resp.LastModified.Format(time.RFC1123)
		lastMod = strings.Replace(lastMod, "UTC", "GMT", 1)
		writer.Header().Set("Last-Modified", lastMod)
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
	if startAfter := request.URL.Query().Get("start-after"); startAfter != "" {
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
			LastModified: content.LastModified.Format("2006-01-02T15:04:05.000Z"),
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
		Marker: resp.Marker,
		KeyCount: int64(len(contents)),
		MaxKeys: resp.MaxKeys,
		IsTruncated: resp.IsTruncated,
		Contents: contents,
		CommonPrefixes: prefixes,
	}
	if resp.Delimiter != nil && *resp.Delimiter != "" {
		s3Resp.Delimiter = resp.Delimiter
	}
	output, _ := xml.Marshal(s3Resp)
	fmt.Printf("Response %s", resp)
	writeLine(xmlHeader, &writer)
	write(string(output), &writer)
	return
}