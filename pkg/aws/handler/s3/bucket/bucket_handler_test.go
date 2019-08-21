package bucket

import (
	"cloud.google.com/go/storage"
	s3_handler "cloudsidecar/pkg/aws/handler/s3"
	"cloudsidecar/pkg/mock"
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"net/http"
	"net/url"
	"testing"
)

func getConfig() *viper.Viper {
	config := viper.New()
	config.Set("gcp_destination_config", "meow")
	return config
}

func TestHandler_ACLParseInput(t *testing.T) {
	valueMap := map[string]string{"bucket": "boops"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	handler := New(nil)
	result, _ := handler.ACLParseInput(req)
	if *result.Bucket != "boops" {
		t.Error("Bucket should be boops")
	}
}

func TestHandler_ACLHandle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bucketMock := s3_handler.NewMockGCPBucket(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	clientMock := s3_handler.NewMockGCPClient(ctrl)
	ctx := context.Background()
	s3Handler := &s3_handler.Handler{
		GCPClient: func() (s3_handler.GCPClient, error) {
			return clientMock, nil
		},
		GCPClientPool: make(map[string][]s3_handler.GCPClient),
		GCPClientToBucket: func(bucket string, client s3_handler.GCPClient) s3_handler.GCPBucket {
			return bucketMock
		},
		Context: &ctx,
		Config:  getConfig(),
	}
	handler := New(s3Handler)
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	aclHandle := storage.ACLHandle{}
	valueMap := map[string]string{"bucket": "boops"}
	req = mux.SetURLVars(req, valueMap)
	bucketMock.EXPECT().ACL().Return(&aclHandle)
	defer recoverFail()
	handler.ACLHandle(writerMock, req)
}

func TestHandler_ListParseInput(t *testing.T) {
	valueMap := map[string]string{"bucket": "boops"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	handler := New(nil)
	result, _ := handler.Listv2ParseInput(req)
	if *result.Bucket != "boops" {
		t.Error("Bucket should be boops")
	}
	if *result.Delimiter != "/" {
		t.Error("Delim should be /")
	}
	if *result.MaxKeys != 1000 {
		t.Error("Max Keys should be 1000")
	}
	if *result.Prefix != "boo" {
		t.Error("Prefix should be boo")
	}
}

func TestHandler_ListHandle_NoBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bucketMock := s3_handler.NewMockGCPBucket(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	s3Handler := &s3_handler.Handler{
		GCPClient: nil,
		GCPClientToBucket: func(bucket string, client s3_handler.GCPClient) s3_handler.GCPBucket {
			return bucketMock
		},
	}
	handler := New(s3Handler)
	request := &http.Request{}
	writerMock.EXPECT().WriteHeader(400)
	writerMock.EXPECT().Write([]byte("no bucket present"))
	handler.ListHandlev2(writerMock, request)
}

func TestHandler_ListHandle_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bucketMock := s3_handler.NewMockGCPBucket(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	clientMock := s3_handler.NewMockGCPClient(ctrl)
	ctx := context.Background()
	s3Handler := &s3_handler.Handler{
		GCPClient: func() (s3_handler.GCPClient, error) {
			return clientMock, nil
		},
		GCPClientPool: make(map[string][]s3_handler.GCPClient),
		GCPClientToBucket: func(bucket string, client s3_handler.GCPClient) s3_handler.GCPBucket {
			return bucketMock
		},
		Context: &ctx,
		Config:  getConfig(),
	}
	handler := New(s3Handler)
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	valueMap := map[string]string{"bucket": "boops"}
	req = mux.SetURLVars(req, valueMap)
	it := storage.ObjectIterator{}
	bucketMock.EXPECT().Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    "boo",
		Versions:  false,
	}).Return(&it)
	listHandlerRecover(handler, writerMock, req)
}

func listHandlerRecover(handler *Handler, w http.ResponseWriter, r *http.Request) {
	defer recoverFail()
	handler.ListHandlev2(w, r)
}

func recoverFail() {
	if r := recover(); r != nil {
		fmt.Println("recovered from ", r)
	}
}
