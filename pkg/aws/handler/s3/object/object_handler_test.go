package object

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	s3_handler "sidecar/pkg/aws/handler/s3"
	"sidecar/pkg/mock"
	"testing"
	"time"
)



func TestHandler_HeadHandle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bucketMock := s3_handler.NewMockGCPBucket(ctrl)
	clientMock := s3_handler.NewMockGCPClient(ctrl)
	objectMock := s3_handler.NewMockGCPObject(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	ctx := context.Background()
	s3Handler := &s3_handler.Handler{
		GCPClient: clientMock,
		GCPClientToBucket: func(bucket string, client s3_handler.GCPClient) s3_handler.GCPBucket {
			return bucketMock
		},
		GCPBucketToObject: func(name string, bucket s3_handler.GCPBucket) s3_handler.GCPObject {
			return objectMock
		},
		Context: &ctx,

	}
	valueMap := map[string]string {"bucket": "boops", "key" : "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	updatedTime := time.Unix(1550463794, 0)
	attrs := &storage.ObjectAttrs{
		Bucket: "boops",
		Name: "mykey",
		Size: 123,
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
	valueMap := map[string]string {"bucket": "boops", "key" : "mykey"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	handler := New(nil)
	result, _ := handler.HeadParseInput(req)
	if *result.Bucket != "boops" {
		t.Error("Bucket should be boops")
	}
	if *result.Key != "mykey" {
		t.Error("Key should be mykey")
	}
}
