package object

import (
	"bytes"
	"cloud.google.com/go/storage"
	s3_handler "cloudsidecar/pkg/aws/handler/s3"
	"cloudsidecar/pkg/mock"
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHandler_PutParseInputChunked(t *testing.T) {
	valueMap := map[string]string{"bucket": "boops", "key": "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	bodyString := fmt.Sprintf(
		"%s\r\n%s\r\n%s\r\n%s\r\n%s\r\n",
		"B;chunk-signature=meowmeowmeow",
		"thisisten\r\n",
		"5;chink-signature=mooo",
		"12345",
		"0;chunk-signature=nope",
	)
	body := ioutil.NopCloser(bytes.NewReader([]byte(bodyString)))
	req := &http.Request{
		URL:    testUrl,
		Body:   body,
		Header: make(http.Header),
	}
	req = mux.SetURLVars(req, valueMap)
	req.Header.Set("Content-Type", "text")
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	handler := New(nil)
	result, _ := handler.PutParseInput(req)
	outputBody, _ := ioutil.ReadAll(result.Body)
	assert.Equal(t, "thisisten\r\n12345", string(outputBody))
}

func TestHandler_PutParseInput(t *testing.T) {
	valueMap := map[string]string{"bucket": "boops", "key": "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	body := ioutil.NopCloser(bytes.NewReader([]byte("enjoy my body")))
	req := &http.Request{
		URL:    testUrl,
		Body:   body,
		Header: make(http.Header),
	}
	req = mux.SetURLVars(req, valueMap)
	req.Header.Set("Content-Type", "text")
	handler := New(nil)
	result, _ := handler.PutParseInput(req)
	if *result.Bucket != "boops" {
		t.Error("Object should be boops")
	}
	if *result.Key != "mykey" {
		t.Error("Key should be mykey")
	}
	outputBody, _ := ioutil.ReadAll(result.Body)
	assert.Equal(t, "enjoy my body", string(outputBody))
	assert.Equal(t, "text", *result.ContentType)
}

func todo_TestHandler_GetHandle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bucketMock := s3_handler.NewMockGCPBucket(ctrl)
	clientMock := s3_handler.NewMockGCPClient(ctrl)
	objectMock := s3_handler.NewMockGCPObject(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	ctx := context.Background()
	s3Handler := &s3_handler.Handler{
		GCPClient: func() (s3_handler.GCPClient, error) {
			return clientMock, nil
		},
		GCPClientPool: make(map[string][]s3_handler.GCPClient),
		GCPClientToBucket: func(bucket string, client s3_handler.GCPClient) s3_handler.GCPBucket {
			return bucketMock
		},
		GCPBucketToObject: func(name string, bucket s3_handler.GCPBucket) s3_handler.GCPObject {
			return objectMock
		},
		Context: &ctx,
	}
	valueMap := map[string]string{"bucket": "boops", "key": "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/boops/mykey")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	attrs := &storage.ObjectAttrs{
		Bucket: "boops",
		Name:   "mykey",
		Size:   123,
	}
	fakeReader := strings.NewReader("this is my file")
	objectMock.EXPECT().Attrs(ctx).Return(attrs, nil)
	objectMock.EXPECT().NewReader(ctx).Return(fakeReader, nil)
	handler := New(s3Handler)
	handler.GetHandle(writerMock, req)
}

func TestHandler_GetParseInput(t *testing.T) {
	valueMap := map[string]string{"bucket": "boops", "key": "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	handler := New(nil)
	result, _ := handler.GetParseInput(req)
	if *result.Bucket != "boops" {
		t.Error("Object should be boops")
	}
	if *result.Key != "mykey" {
		t.Error("Key should be mykey")
	}
}

func TestHandler_HeadHandle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bucketMock := s3_handler.NewMockGCPBucket(ctrl)
	clientMock := s3_handler.NewMockGCPClient(ctrl)
	objectMock := s3_handler.NewMockGCPObject(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	ctx := context.Background()
	s3Handler := &s3_handler.Handler{
		GCPClient: func() (s3_handler.GCPClient, error) {
			return clientMock, nil
		},
		GCPClientPool: make(map[string][]s3_handler.GCPClient),
		GCPClientToBucket: func(bucket string, client s3_handler.GCPClient) s3_handler.GCPBucket {
			return bucketMock
		},
		GCPBucketToObject: func(name string, bucket s3_handler.GCPBucket) s3_handler.GCPObject {
			return objectMock
		},
		Context: &ctx,
	}
	valueMap := map[string]string{"bucket": "boops", "key": "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	updatedTime := time.Unix(1550463794, 0)
	attrs := &storage.ObjectAttrs{
		Bucket:  "boops",
		Name:    "mykey",
		Size:    123,
		Updated: updatedTime,
	}
	outputMap := make(http.Header)
	objectMock.EXPECT().Attrs(ctx).Return(attrs, nil)
	writerMock.EXPECT().Header().Times(2).Return(outputMap)
	writerMock.EXPECT().WriteHeader(200)
	handler := New(s3Handler)
	handler.HeadHandle(writerMock, req)
	assert.Equal(t, "123", outputMap.Get("Content-Length"))
	assert.Equal(t, "Mon, 18 Feb 2019 04:23:14 GMT", outputMap.Get("Last-Modified"))
}

func TestHandler_HeadParseInput(t *testing.T) {
	valueMap := map[string]string{"bucket": "boops", "key": "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	handler := New(nil)
	result, _ := handler.HeadParseInput(req)
	if *result.Bucket != "boops" {
		t.Error("Object should be boops")
	}
	if *result.Key != "mykey" {
		t.Error("Key should be mykey")
	}
}
