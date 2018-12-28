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

const (
	xmlHeader string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
)

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
	req := handler.S3Client.ListObjectsRequest(&s3.ListObjectsInput{Bucket: &bucket})
	resp, respError := req.Send()
	if respError != nil {
		panic(fmt.Sprintf("Error %s", respError))
	}
	fmt.Printf("Response %s", resp)
	_, err := writer.Write([]byte("test"))
	if err != nil {
		panic(fmt.Sprintf("Error %s", err))
	}
	return
}