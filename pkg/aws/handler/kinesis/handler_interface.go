package kinesis

import (
	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/spf13/viper"
)

type Handler struct {
	KinesisClient *kinesis.Kinesis
	GCPClient GCPClient
	GCPClientToTopic func(topic string, client GCPClient) GCPTopic
	GCPResultWrapper func(result *pubsub.PublishResult) GCPPublishResult
	GCPKMSClient *kms.KeyManagementClient
	Context *context.Context
	Config *viper.Viper
}

type HandlerInterface interface {
	GetKinesisClient() *kinesis.Kinesis
	GetGCPClient() GCPClient
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetKinesisClient(kinesisClient *kinesis.Kinesis)
	SetGCPClient(gcpClient GCPClient)
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)
}

func (handler *Handler) GetKinesisClient() *kinesis.Kinesis {
	return handler.KinesisClient
}
func (handler *Handler) GetGCPClient() GCPClient {
	return handler.GCPClient
}
func (handler *Handler) GetContext() *context.Context{
	return handler.Context
}
func (handler *Handler) GetConfig() *viper.Viper {
	return handler.Config
}
func (handler *Handler) SetKinesisClient(kinesisClient *kinesis.Kinesis){
	handler.KinesisClient = kinesisClient
}
func (handler *Handler) SetGCPClient(gcpClient GCPClient) {
	handler.GCPClient = gcpClient
}
func (handler *Handler) SetContext(context *context.Context) {
	handler.Context = context
}
func (handler *Handler) SetConfig(config *viper.Viper) {
	handler.Config = config
}

type GCPClient interface {
	CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
	Subscription(id string) *pubsub.Subscription
	Topic(id string) *pubsub.Topic
	CreateTopic(ctx context.Context, id string) (*pubsub.Topic, error)
	Close() error
}

type GCPTopic interface {
	Delete(ctx context.Context) error
	Config(ctx context.Context) (pubsub.TopicConfig, error)
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
	Stop()
}

type GCPPublishResult interface {
	Ready() <-chan struct{}
	Get(ctx context.Context) (serverID string, err error)
}
