package s3

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/spf13/viper"
	"strings"
	"time"
)

type Handler struct {
	S3Client          s3iface.S3API
	GCPClient         GCPClient
	Context           *context.Context
	Config            *viper.Viper
	GCPClientToBucket func(bucket string, client GCPClient) GCPBucket
	GCPBucketToObject func(name string, bucket GCPBucket) GCPObject
}

type GCPClient interface {
	Bucket(name string) *storage.BucketHandle
}

type GCPObject interface {
	ACL() *storage.ACLHandle
	Generation(gen int64) *storage.ObjectHandle
	If(conds storage.Conditions) *storage.ObjectHandle
	Key(encryptionKey []byte) *storage.ObjectHandle
	Attrs(ctx context.Context) (attrs *storage.ObjectAttrs, err error)
	Update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (oa *storage.ObjectAttrs, err error)
	BucketName() string
	ObjectName() string
	Delete(ctx context.Context) error
	ReadCompressed(compressed bool) *storage.ObjectHandle
	NewWriter(ctx context.Context) *storage.Writer
	NewReader(ctx context.Context) (GCPReader, error)
	NewRangeReader(ctx context.Context, offset, length int64) (r GCPReader, err error)
	CopierFrom(src *storage.ObjectHandle) *storage.Copier
	ComposerFrom(srcs ...*storage.ObjectHandle) *storage.Composer
}

type GCPReader interface {
	Close() error
	Read(p []byte) (int, error)
	Size() int64
	Remain() int64
	ContentType() string
	ContentEncoding() string
	CacheControl() string
	LastModified() (time.Time, error)
}

type GCPBucket interface {
	Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error)
	Delete(ctx context.Context) (err error)
	ACL() *storage.ACLHandle
	DefaultObjectACL() *storage.ACLHandle
	Object(name string) *storage.ObjectHandle
	Attrs(ctx context.Context) (attrs *storage.BucketAttrs, err error)
	Update(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (attrs *storage.BucketAttrs, err error)
	If(conds storage.BucketConditions) *storage.BucketHandle
	UserProject(projectID string) *storage.BucketHandle
	LockRetentionPolicy(ctx context.Context) error
	Objects(ctx context.Context, q *storage.Query) *storage.ObjectIterator
}

type HandlerInterface interface {
	GetS3Client() s3iface.S3API
	GetGCPClient() GCPClient
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetS3Client(s3Client s3iface.S3API)
	SetGCPClient(gcpClient GCPClient)
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)
}

func (handler *Handler) GetS3Client() s3iface.S3API {
	return handler.S3Client
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
func (handler *Handler) SetS3Client(s3Client s3iface.S3API){
	handler.S3Client = s3Client
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

func (handler *Handler) BucketRename(bucket string) string {
	if handler.Config != nil {
		renameMap := handler.Config.GetStringMapString("gcp_destination_config.gcs_config.bucket_rename")
		bucket = strings.Replace(bucket, ".",  "__dot__",  -1)
		if renameMap != nil {
			if val, ok := renameMap[bucket]; ok {
				return val
			}
		}
		return bucket
	} else {
		return bucket
	}
}

const (
	XmlHeader string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
)

