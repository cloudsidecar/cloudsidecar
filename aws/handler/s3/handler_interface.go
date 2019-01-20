package s3

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/viper"
)

type Handler struct {
	S3Client *s3.S3
	GCPClient *storage.Client
	Context *context.Context
	Config *viper.Viper
}

type HandlerInterface interface {
	GetS3Client() *s3.S3
	GetGCPClient() *storage.Client
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetS3Client(s3Client *s3.S3)
	SetGCPClient(gcpClient *storage.Client)
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)
}

func (handler *Handler) GetS3Client() *s3.S3 {
	return handler.S3Client
}
func (handler *Handler) GetGCPClient() *storage.Client {
	return handler.GCPClient
}
func (handler *Handler) GetContext() *context.Context{
	return handler.Context
}
func (handler *Handler) GetConfig() *viper.Viper {
	return handler.Config
}
func (handler *Handler) SetS3Client(s3Client *s3.S3){
	handler.S3Client = s3Client
}
func (handler *Handler) SetGCPClient(gcpClient *storage.Client) {
	handler.GCPClient = gcpClient
}
func (handler *Handler) SetContext(context *context.Context) {
	handler.Context = context
}
func (handler *Handler) SetConfig(config *viper.Viper) {
	handler.Config = config
}

func (handler *Handler) BucketRename(bucket string) string {
	renameMap := handler.Config.GetStringMapString("gcp_destination_config.gcs_config.bucket_rename")
	fmt.Println("Looking for", bucket, "in", renameMap)
	if renameMap != nil {
		if val, ok := renameMap[bucket]; ok {
			return val
		}
	}
	return bucket
}

const (
	XmlHeader string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
)

