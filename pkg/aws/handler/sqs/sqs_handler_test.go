package sqs

import (
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/aws/handler/kinesis"
	"cloudsidecar/pkg/mock"
	"context"
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

func TestHandler_SendHandleParseInput(t *testing.T) {
	handler := NewHandler(getConfig())
	testUrl, _ := url.ParseRequestURI("http://localhost:3450")
	req := &http.Request{
		URL:  testUrl,
		Form: url.Values{
			"QueueUrl": []string{"woof"},
			"MessageBody": []string{"this is my body"},
			"MessageAttribute.1.Name": []string{"my_attribute_name_1"},
			"MessageAttribute.1.Value.StringValue": []string{"my_attribute_value_1"},
			"MessageAttribute.1.Value.DataType": []string{"String"},
			"MessageAttribute.2.Name": []string{"my_attribute_name_2"},
			"MessageAttribute.2.Value.StringValue": []string{"my_attribute_value_2"},
			"MessageAttribute.2.Value.DataType": []string{"String"},
		},
	}
	input, _ := handler.SendHandleParseInput(req)
	assert.Equal(t, *input.QueueUrl, "woof")
	assert.Equal(t, *input.MessageBody, "this is my body")
	firstAttribute := input.MessageAttributes["my_attribute_name_1"]
	assert.Equal(t, *firstAttribute.DataType, "String")
	assert.Equal(t, *firstAttribute.StringValue, "my_attribute_value_1")

	secondAttribute := input.MessageAttributes["my_attribute_name_2"]
	assert.Equal(t, *secondAttribute.DataType, "String")
	assert.Equal(t, *secondAttribute.StringValue, "my_attribute_value_2")
}

func TestHandler_SendHandle(t *testing.T) {
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
		Form: url.Values{
			"QueueUrl": []string{"myQueue/queuename"},
			"MessageBody": []string{"bvrp"},
		},
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := kinesis.NewMockGCPClient(ctrl)
	mockGcpTopic := kinesis.NewMockGCPTopic(ctrl)
	mockGcpPublishResult := kinesis.NewMockGCPPublishResult(ctrl)
	writerMock := mock.NewMockResponseWriter(ctrl)
	handler := NewHandler(getConfig())
	ctx := context.Background()
	message := &pubsub.Message{
		Data: []byte("bvrp"),
	}
	mockGcpTopic.EXPECT().Publish(ctx, message).Return(&pubsub.PublishResult{})
	mockGcpTopic.EXPECT().Stop()
	mockGcpPublishResult.EXPECT().Get(ctx).Return("meowid", nil)
	handler.GCPClient = mockGcpClient
	handler.GCPClientToTopic =  func(topic string, client kinesis.GCPClient) kinesis.GCPTopic {
		return mockGcpTopic
	}
	handler.GCPResultWrapper = func(result *pubsub.PublishResult) kinesis.GCPPublishResult {
		return mockGcpPublishResult
	}
	handler.Context = &ctx
	writerMock.EXPECT().Write([]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"))
	writerMock.EXPECT().Write([]byte("<SendMessageResponse xmlns=\"http://www.w3.org/2001/XMLSchema-instance\"><SendMessageResult><MD5OfMessageBody>464727132a6350c4db1726435f70c35c</MD5OfMessageBody><MessageId>meowid</MessageId></SendMessageResult></SendMessageResponse>"))
	handler.SendHandle(writerMock, req)
}

func TestHandler_ReceiveHandleParseInput(t *testing.T) {
	handler := NewHandler(getConfig())
	testUrl, _ := url.ParseRequestURI("http://localhost:3450")
	req := &http.Request{
		URL:  testUrl,
		Form: url.Values{
			"QueueUrl": []string{"woof"},
			"VisibilityTimeout": []string{"50"},
		},
	}
	input, _ := handler.ReceiveHandleParseInput(req)
	assert.Equal(t, *input.QueueUrl, "woof")
	assert.Equal(t, *input.MaxNumberOfMessages, int64(10))
	assert.Equal(t, *input.VisibilityTimeout, int64(50))
}
