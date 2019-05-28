package kinesis

import (
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/mock"
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestKinesisHandler_CreateStreamParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL: testUrl,
		Body:fakeReader,
	}
	handler := New(nil)
	result, _ := handler.CreateStreamParseInput(req)
	assert.Equal(t, "my_stream", *result.StreamName)
}

func TestKinesisHandler_CreateStreamHandle(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL: testUrl,
		Body:fakeReader,
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := NewMockGCPClient(ctrl)
	mockGcpClient.EXPECT().CreateTopic(ctx, "my_stream").Return(&pubsub.Topic{

	}, nil)
	handler := &KinesisHandler{
		&Handler{
			GCPClient: mockGcpClient,
			Context: &ctx,
		},
	}
	writerMock := mock.NewMockResponseWriter(ctrl)
	writerMock.EXPECT().WriteHeader(200)
	handler.CreateStreamHandle(writerMock, req)
}