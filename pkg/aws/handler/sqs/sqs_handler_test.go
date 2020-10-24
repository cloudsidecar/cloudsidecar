package sqs

import (
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/aws/handler/kinesis"
	"cloudsidecar/pkg/mock"
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func getConfig() *viper.Viper {
	config := viper.New()
	config.Set("gcp_destination_config", "meow")
	return config
}

func TestHandler_CreateHandle(t *testing.T) {
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
		Form: url.Values{"QueueName": []string{"myQueue"}},
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := kinesis.NewMockGCPClient(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	handler := NewHandler(getConfig())
	ctx := context.Background()
	mockGcpClient.EXPECT().CreateTopic(ctx, "myQueue").Return(&pubsub.Topic{}, nil)
	mockGcpClient.EXPECT().Topic("myQueue").Return(&pubsub.Topic{})
	ackDuration, _ := time.ParseDuration("1m")
	retentionDuration, _ := time.ParseDuration("1d")
	config := pubsub.SubscriptionConfig{
		Topic:             &pubsub.Topic{},
		AckDeadline:       ackDuration,
		RetentionDuration: retentionDuration,
	}
	mockGcpClient.EXPECT().CreateSubscription(ctx, "myQueue", config).Return(nil, nil)
	handler.GCPClient = mockGcpClient
	handler.Context = &ctx
	fmt.Printf("huh %s\n", handler.GCPClient)
	writerMock.EXPECT().Write([]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"))
	writerMock.EXPECT().Write([]byte("<CreateQueueResponse xmlns=\"http://www.w3.org/2001/XMLSchema-instance\"><CreateQueueResult><QueueUrl>https://sqs/myQueue</QueueUrl></CreateQueueResult></CreateQueueResponse>"))
	handler.CreateHandle(writerMock, req)
}

func TestHandler_CreateHandleParseInput(t *testing.T) {
	handler := NewHandler(getConfig())
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	req := &http.Request{
		URL:  testUrl,
		Form: url.Values{"QueueName": []string{"myQueue"}, "Attribute.1.Name": []string{"IsMeow"}, "Attribute.1.Value": []string{"val1"}},
	}
	input, _ := handler.CreateHandleParseInput(req)
	assert.Equal(t, *input.QueueName, "myQueue")
	assert.Equal(t, input.Attributes["IsMeow"], "val1")
}

func TestHandler_DeleteHandle(t *testing.T) {
	// GCP delete doesn't do anything
}

func TestHandler_DeleteHandleParseInput(t *testing.T) {
	handler := NewHandler(getConfig())
	testUrl, _ := url.ParseRequestURI("http://localhost:3450")
	req := &http.Request{
		URL:  testUrl,
		Form: url.Values{"QueueUrl": []string{"woof"}},
	}
	input, _ := handler.DeleteHandleParseInput(req)
	assert.Equal(t, *input.QueueUrl, "woof")
}
