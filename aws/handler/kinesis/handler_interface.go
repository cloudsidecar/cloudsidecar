package kinesis

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/spf13/viper"
)

type Handler struct {
	KinesisClient *kinesis.Kinesis
	GCPClient *pubsub.Client
	Context *context.Context
	Config *viper.Viper
}

type HandlerInterface interface {
	GetKinesisClient() *kinesis.Kinesis
	GetGCPClient() *pubsub.Client
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetKinesisClient(kinesisClient *kinesis.Kinesis)
	SetGCPClient(gcpClient *pubsub.Client)
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)
}

func (handler *Handler) GetKinesisClient() *kinesis.Kinesis {
	return handler.KinesisClient
}
func (handler *Handler) GetGCPClient() *pubsub.Client {
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
func (handler *Handler) SetGCPClient(gcpClient *pubsub.Client) {
	handler.GCPClient = gcpClient
}
func (handler *Handler) SetContext(context *context.Context) {
	handler.Context = context
}
func (handler *Handler) SetConfig(config *viper.Viper) {
	handler.Config = config
}
