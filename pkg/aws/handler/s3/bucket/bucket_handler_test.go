package bucket

import (
	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"testing"
	s3_handler "sidecar/pkg/aws/handler/s3"
)

type MockGCPClient struct {
}

type MockGCPBucket struct {
	*storage.BucketHandle
}

func (MockGCPClient) Bucket(name string) s3_handler.GCPBucket {
	return MockGCPBucket{}
}

func TestHandler_ListParseInput(t *testing.T) {
	valueMap := map[string]string {"bucket": "boops"}
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/beh?list-type=2&prefix=boo&delimiter=%2F&encoding-type=url")
	req := &http.Request{
		URL: testUrl,
	}
	req = mux.SetURLVars(req, valueMap)
	handler := New(nil)
	result, _ := handler.ListParseInput(req)
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

func TestHandler_ListHandle(t *testing.T) {
	s3Handler := &s3_handler.Handler{
		GCPClient: nil,
	}
	handler := New(s3Handler)
	handler.GCPClient.Bucket()
}
