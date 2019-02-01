package dynamo

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/viper"
)

type Handler struct {
	DynamoClient      *dynamodb.DynamoDB
	GCPBigTableClient *bigtable.Client
	GCPDatastoreClient *datastore.Client
	Context *context.Context
	Config *viper.Viper
}

type HandlerInterface interface {
	GetDynamoClient() *dynamodb.DynamoDB
	GetGCPBigTableClient() *bigtable.Client
	GetGCPDatastoreClient() *datastore.Client
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetDynamoClient(client *dynamodb.DynamoDB)
	SetGCPBigTableClient(client *bigtable.Client)
	SetGCPDatastoreClient(client *datastore.Client)
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)
}

func (handler *Handler) GetDynamoClient() *dynamodb.DynamoDB {
	return handler.DynamoClient
}
func (handler *Handler) GetGCPBigTableClient() *bigtable.Client {
	return handler.GCPBigTableClient
}
func (handler *Handler) GetGCPDatastoreClient() *datastore.Client {
	return handler.GCPDatastoreClient
}
func (handler *Handler) GetContext() *context.Context {
	return handler.Context
}
func (handler *Handler) GetConfig() *viper.Viper {
	return handler.Config
}
func (handler *Handler) SetDynamoClient(client *dynamodb.DynamoDB) {
	handler.DynamoClient = client
}
func (handler *Handler) SetGCPBigTableClient(client *bigtable.Client) {
	handler.GCPBigTableClient = client
}
func (handler *Handler) SetGCPDatastoreClient(client *datastore.Client) {
	handler.GCPDatastoreClient = client
}
func (handler *Handler) SetContext(context *context.Context) {
	handler.Context = context
}
func (handler *Handler) SetConfig(config *viper.Viper) {
	handler.Config = config
}
