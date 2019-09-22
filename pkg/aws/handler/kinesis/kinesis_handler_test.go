package kinesis

import (
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/mock"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func getConfig() *viper.Viper {
	config := viper.New()
	config.Set("gcp_destination_config", "meow")
	return config
}

func TestKinesisHandler_CreateStreamParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	handler := New(nil)
	result, _ := handler.CreateStreamParseInput(req)
	assert.Equal(t, "my_stream", *result.StreamName)
}

func TestKinesisHandler_CreateStreamHandle(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := NewMockGCPClient(ctrl)
	mockGcpClient.EXPECT().CreateTopic(ctx, "my_stream").Return(&pubsub.Topic{}, nil)
	handler := &KinesisHandler{
		&Handler{
			GCPClient: mockGcpClient,
			Context:   &ctx,
			Config: getConfig(),
		},
	}
	writerMock := mock.NewMockResponseWriter(ctrl)
	writerMock.EXPECT().WriteHeader(200)
	handler.CreateStreamHandle(writerMock, req)
}

func TestKinesisHandler_DeleteStreamParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	handler := New(nil)
	result, _ := handler.DeleteStreamParseInput(req)
	assert.Equal(t, "my_stream", *result.StreamName)
}

func TestKinesisHandler_DeleteStreamHandle(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := NewMockGCPClient(ctrl)
	mockGcpTopic := NewMockGCPTopic(ctrl)
	mockGcpTopic.EXPECT().Delete(ctx).Return(nil)
	handler := &KinesisHandler{
		&Handler{
			GCPClient: mockGcpClient,
			GCPClientToTopic: func(topic string, client GCPClient) GCPTopic {
				return mockGcpTopic
			},
			Context: &ctx,
			Config: getConfig(),
		},
	}
	writerMock := mock.NewMockResponseWriter(ctrl)
	writerMock.EXPECT().WriteHeader(200)
	handler.DeleteStreamHandle(writerMock, req)
}

func TestKinesisHandler_DescribeParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	handler := New(nil)
	result, _ := handler.DescribeParseInput(req)
	assert.Equal(t, "my_stream", *result.StreamName)
}

func TestKinesisHandler_DescribeHandle(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := NewMockGCPClient(ctrl)
	mockGcpTopic := NewMockGCPTopic(ctrl)
	mockGcpTopic.EXPECT().Config(ctx).Return(pubsub.TopicConfig{}, nil)
	handler := &KinesisHandler{
		&Handler{
			GCPClient: mockGcpClient,
			GCPClientToTopic: func(topic string, client GCPClient) GCPTopic {
				return mockGcpTopic
			},
			Context: &ctx,
			Config: getConfig(),
		},
	}
	writerMock := mock.NewMockResponseWriter(ctrl)
	responseJson := "{\"StreamDescription\":{\"EncryptionType\":\"\",\"EnhancedMonitoring\":null,\"HasMoreShards\":false,\"KeyId\":null,\"RetentionPeriodHours\":24,\"Shards\":[{\"AdjacentParentShardId\":null,\"HashKeyRange\":{\"EndingHashKey\":\"340282366920938463463374607431768211455\",\"StartingHashKey\":\"0\"},\"ParentShardId\":null,\"SequenceNumberRange\":{\"EndingSequenceNumber\":null,\"StartingSequenceNumber\":\"0\"},\"ShardId\":\"shard-0\"}],\"StreamARN\":null,\"StreamCreationTimestamp\":null,\"StreamName\":\"my_stream\",\"StreamStatus\":\"ACTIVE\"}}\n"
	writerMock.EXPECT().Write([]byte(responseJson))
	handler.DescribeHandle(writerMock, req)
}

func TestKinesisHandler_StartStreamEncryptionParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	handler := New(nil)
	result, _ := handler.StartStreamEncryptionParseInput(req)
	assert.Equal(t, "my_stream", *result.StreamName)
}

func TestKinesisHandler_StartStreamEncryptionHandle(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := NewMockGCPClient(ctrl)
	handler := &KinesisHandler{
		&Handler{
			GCPClient: mockGcpClient,
			Context:   &ctx,
			Config: getConfig(),
		},
	}
	writerMock := mock.NewMockResponseWriter(ctrl)
	writerMock.EXPECT().WriteHeader(200)
	handler.StartStreamEncryptionHandle(writerMock, req)
}

func TestKinesisHandler_PublishParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"streamname\" : \"my_stream\", \"data\": \"bvrp\"}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	handler := New(nil)
	result, _ := handler.PublishParseInput(req)
	assert.Equal(t, "my_stream", result.StreamName)
	assert.Equal(t, "bvrp", result.Data)
}

func TestKinesisHandler_PublishHandle(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	bvrp := base64.StdEncoding.EncodeToString([]byte("bvrp"))
	messageJson := fmt.Sprintf("{\"streamname\" : \"my_stream\", \"data\": \"%s\"}", bvrp)
	fakeReader := ioutil.NopCloser(strings.NewReader(messageJson))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGcpClient := NewMockGCPClient(ctrl)
	mockGcpTopic := NewMockGCPTopic(ctrl)
	mockGcpPublishResult := NewMockGCPPublishResult(ctrl)
	mockGcpPublishResult.EXPECT().Get(ctx).Return("meowid", nil)
	message := &pubsub.Message{
		Data: []byte("bvrp"),
	}
	mockGcpTopic.EXPECT().Publish(ctx, message).Return(&pubsub.PublishResult{})
	mockGcpTopic.EXPECT().Stop()
	handler := &KinesisHandler{
		&Handler{
			GCPClient: mockGcpClient,
			GCPClientToTopic: func(topic string, client GCPClient) GCPTopic {
				return mockGcpTopic
			},
			GCPResultWrapper: func(result *pubsub.PublishResult) GCPPublishResult {
				return mockGcpPublishResult
			},
			Context: &ctx,
			Config: getConfig(),
		},
	}
	writerMock := mock.NewMockResponseWriter(ctrl)
	responseJson := "{\"SequenceNumber\":\"meowid\",\"ShardId\":\"shard-0\",\"ErrorCode\":null,\"ErrorMessage\":null}\n"
	writerMock.EXPECT().Write([]byte(responseJson))
	handler.PublishHandle(writerMock, req)
}

func TestKinesisHandler_GetRecordsParseInput(t *testing.T) {
	testUrl, _ := url.ParseRequestURI("http://localhost:3450/")
	fakeReader := ioutil.NopCloser(strings.NewReader("{\"ShardIterator\" : \"my_shard\", \"Limit\": 123}"))
	req := &http.Request{
		URL:  testUrl,
		Body: fakeReader,
	}
	handler := New(nil)
	result, _ := handler.GetRecordsParseInput(req)
	assert.Equal(t, "my_shard", *result.ShardIterator)
	assert.Equal(t, int64(123), *result.Limit)
}
