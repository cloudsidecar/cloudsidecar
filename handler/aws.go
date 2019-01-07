package handler

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"sidecar/converter"
	"sidecar/response_type"
	"strconv"
	"strings"
	"time"
)

type S3Handler struct {
	S3Client *s3.S3
	GCPClient *storage.Client
	Context *context.Context
}


type ChunkedReaderWrapper struct {
	Reader            *io.ReadCloser
	ContentLength     *int64
	Buffer            []byte
	ChunkNextPosition int
	ChunkSize         int
}

func (wrapper ChunkedReaderWrapper) ReadHeaderGetChunkSize() (i int, err error) {
	chunkedHeader, err := wrapper.ReadHeader()
	if err != nil {
		fmt.Printf("Error reading header %s", err)
		return 0, err
	}
	fmt.Printf("Read header %s\n", chunkedHeader)
	chunkedSplit := strings.SplitN(chunkedHeader, ";", 2)
	chunkSize, err := strconv.ParseInt(chunkedSplit[0], 16, 32)
	return int(chunkSize), err
}

func (wrapper ChunkedReaderWrapper) ReadHeader() (s string, err error) {
	oneByte := make([]byte, 1)
	readCount := 0
	header := make([]byte, 4096)
	for {
		_, err := io.ReadFull(*wrapper.Reader, oneByte)
		if err != nil {
			return string(header[:readCount]), err
		}
		if oneByte[0] == '\r' {
			// read \n
			io.ReadFull(*wrapper.Reader, oneByte)
			if readCount != 0 {
				return string(header[:readCount]), nil
			} else {
				// \r is first char
				io.ReadFull(*wrapper.Reader, oneByte)
			}
		}
		if readCount >= len(header) {
			return string(header[:readCount]), io.ErrShortBuffer
		}
		header[readCount] = oneByte[0]
		readCount++
	}
}

func (wrapper *ChunkedReaderWrapper) Read(p []byte) (n int, err error) {
	if wrapper.Buffer == nil || len(wrapper.Buffer) == 0 {
		wrapper.ChunkNextPosition = 0
		chunkSize, err := wrapper.ReadHeaderGetChunkSize()
		fmt.Printf("Chunk size %d\n", chunkSize)
		if err != nil {
			fmt.Printf("Error reading header %s", err)
			return 0, err
		}
		wrapper.ChunkSize = chunkSize
		if chunkSize == 0 {
			return 0, io.EOF
		}
		buffer := make([]byte, chunkSize)
		wrapper.Buffer = buffer
		_, err = io.ReadFull(*wrapper.Reader, buffer)
		if err != nil {
			fmt.Printf("Error reading all %s", err)
			return 0, err
		}
	}
	// 0: wrapper.Buffer = 5, CNP = 0, bytesLeft = 5
	// pSize = 2
	// read [0, 2]
	// 1: CNP = 2, bytesLeft = 3
	// read [2, 4]
	// 2: CNP = 4, bytesLeft = 1
	bytesLeft := len(wrapper.Buffer) - wrapper.ChunkNextPosition
	pSize := len(p)
	if pSize <= bytesLeft {
		nextPos := wrapper.ChunkNextPosition + pSize
		copy(p, (wrapper.Buffer)[wrapper.ChunkNextPosition:nextPos])
		wrapper.ChunkNextPosition = nextPos
		fmt.Println("READO ", pSize, bytesLeft, len(wrapper.Buffer))
		return pSize, nil
	} else {
		fmt.Println("DONE READO ", pSize, bytesLeft, len(wrapper.Buffer))
		n := copy(p, wrapper.Buffer[wrapper.ChunkNextPosition:])
		wrapper.Buffer = nil
		return n, nil
	}
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
	if handler.GCPClient != nil {
		acl := handler.GCPClient.Bucket(bucket).ACL()
		aclList, err := acl.List(*handler.Context)
		if err != nil {
			fmt.Printf("Error with GCP %s", err)
			writer.WriteHeader(404)
			return
		}
		output, _ := xml.MarshalIndent(converter.GCSACLResponseToAWS(aclList), "  ", "    ")
		fmt.Printf("Response %s", aclList)
		writeLine(xmlHeader, &writer)
		writeLine(string(output), &writer)
	} else {
		req := handler.S3Client.GetBucketAclRequest(&s3.GetBucketAclInput{Bucket: &bucket})
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
		writeLine(xmlHeader, &writer)
		writeLine(string(output), &writer)
		return
	}
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
	isChunked := false
	if header := request.Header.Get("x-amz-content-sha256"); header == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		isChunked = true
	}
	var contentLength int64
	if header := request.Header.Get("x-amz-decoded-content-length"); header != "" {
		contentLength, _ = strconv.ParseInt(header, 10, 64)
	} else if header := request.Header.Get("x-amz-decoded-content-length"); header != "" {
		contentLength, _ = strconv.ParseInt(header, 10, 64)
	}
	defer request.Body.Close()
	var err error
	if !isChunked {
		s3Req.Body = request.Body
	} else {
		fmt.Printf("CHUNKED %d", contentLength)
		readerWrapper := ChunkedReaderWrapper{
			Reader:         &request.Body,
			ContentLength:  &contentLength,
		}
		s3Req.Body = &readerWrapper
	}
	// wg := sync.WaitGroup{}
	if handler.GCPClient != nil {
		uploader := handler.GCPClient.Bucket(bucket).Object(key).NewWriter(*handler.Context)
		defer uploader.Close()
		_, err := converter.GCPUpload(s3Req, uploader)
		if err != nil {
			fmt.Printf("\nBOOO Error %s\n", err)
		}
	} else {
		uploader := s3manager.NewUploaderWithClient(handler.S3Client)
		_, err = uploader.Upload(s3Req)
		if err != nil {
			fmt.Printf("Error %s", err)
		}
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
	if handler.GCPClient != nil {
		objHandle := handler.GCPClient.Bucket(bucket).Object(key)
		attrs, err := objHandle.Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", err)
			return
		}
		converter.GCSAttrToHeaders(attrs, writer)
		var reader *storage.Reader
		var readerError error
		if s3Req.Range != nil {
			equalSplit := strings.SplitN(*s3Req.Range, "=", 2)
			byteSplit := strings.SplitN(equalSplit[1], "-", 2)
			startByte, _ := strconv.ParseInt(byteSplit[0], 10, 64)
			length := int64(-1)
			if len(byteSplit) > 1 {
				endByte, _ := strconv.ParseInt(byteSplit[1], 10, 64)
				length = endByte + 1 - startByte
			}
			reader, readerError = objHandle.NewRangeReader(*handler.Context, startByte, length)
		} else {
			reader, readerError = objHandle.NewReader(*handler.Context)
		}
		if readerError != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", readerError)
			return
		}
		defer reader.Close()
		buffer := make([]byte, 4096)
		for {
			n, err := reader.Read(buffer)
			if n > 0 {
				writeBytes(buffer[:n], &writer)
			}
			if err == io.EOF {
				break
			}
		}
	} else {
		req := handler.S3Client.GetObjectRequest(s3Req)
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
	}
	return
}

func (handler S3Handler) S3HeadFile(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	bucket := vars["bucket"]
	key := vars["key"]
	fmt.Printf("Looking for %s %s\n", bucket, key)
	fmt.Printf("URL %s\n", request.URL)
	if handler.GCPClient != nil {
		resp, err := handler.GCPClient.Bucket(bucket).Object(key).Attrs(*handler.Context)
		if err != nil {
			writer.WriteHeader(404)
			fmt.Printf("Error %s", err)
			return
		}
		fmt.Printf("Response %s\n", *resp)
		converter.GCSAttrToHeaders(resp, writer)
	} else {
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
	}
	writer.WriteHeader(200)
	return
}

func (handler S3Handler) S3List(writer http.ResponseWriter, request *http.Request) {
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
	if handler.GCPClient != nil {
		bucketObject := handler.GCPClient.Bucket(bucket)
		it := bucketObject.Objects(*handler.Context, &storage.Query{
			Delimiter: *listRequest.Delimiter,
			Prefix: *listRequest.Prefix,
			Versions: false,
		})
		s3Resp := converter.GCSListResponseToAWS(it, listRequest)
		output, _ := xml.MarshalIndent(s3Resp, "  ", "    ")
		fmt.Printf("Response %s", output)
		writeLine(xmlHeader, &writer)
		writeLine(string(output), &writer)
	} else {

		fmt.Printf("Requesting %s", listRequest)
		req := handler.S3Client.ListObjectsRequest(listRequest)
		resp, respError := req.Send()
		if respError != nil {
			panic(fmt.Sprintf("Error %s", respError))
		}
		var contents = make([]*response_type.BucketContent, len(resp.Contents))
		for i, content := range resp.Contents {
			contents[i] = &response_type.BucketContent{
				Key: *content.Key,
				LastModified: content.LastModified.Format("2006-01-02T15:04:05.000Z"),
				ETag: *content.ETag,
				Size: *content.Size,
				StorageClass: string(content.StorageClass),
			}
		}
		var prefixes = make([]*response_type.BucketCommonPrefix, len(resp.CommonPrefixes))
		for i, prefix := range resp.CommonPrefixes {
			prefixes[i] = &response_type.BucketCommonPrefix{
				Prefix: *prefix.Prefix,
			}
		}
		s3Resp := &response_type.AWSListBucketResponse{
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
			NextContinuationToken: resp.NextMarker,
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
}