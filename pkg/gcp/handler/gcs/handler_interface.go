package gcs

import (
	"cloud.google.com/go/storage"
	"cloudsidecar/pkg/logging"
	"context"
	"encoding/base64"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"cloudsidecar/pkg/aws/handler/s3"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
	"net/http"
	"sync"
)

type Handler struct {
	S3Client          s3iface.S3API
	GCPClient         func() (s3.GCPClient, error)
	Context           *context.Context
	Config            *viper.Viper
	GCPClientToBucket func(bucket string, client s3.GCPClient) s3.GCPBucket
	GCPBucketToObject func(name string, bucket s3.GCPBucket) s3.GCPObject
	GCPObjectToWriter func(object s3.GCPObject, ctx context.Context) s3.GCPObjectWriter
	GCPClientPerKey   map[string]s3.GCPClient
	gcpClientMapLock  sync.Mutex
	GCPClientPool     map[string][]s3.GCPClient
	gcpClientPoolLock sync.Mutex
}

func (handler *Handler) GetConnection(key string) (s3.GCPClient, error) {
	handler.gcpClientPoolLock.Lock()
	defer handler.gcpClientPoolLock.Unlock()
	var client s3.GCPClient
	var err error
	if handler.GCPClientPool[key] == nil || len(handler.GCPClientPool[key]) == 0 {
		if key != "" {
			client, err = handler.SetGCPClientFromCreds(&key)
		} else {
			client, err = handler.GCPClient()
		}
		handler.GCPClientPool[key] = []s3.GCPClient{client}
	} else {
		client = handler.GCPClientPool[key][0]
		err = nil
		handler.GCPClientPool[key] = handler.GCPClientPool[key][1:]
	}
	return client, err
}

func (handler *Handler) GCPRequestSetup(request *http.Request) (s3.GCPClient, error) {
	logging.LogUsingGCP()
	if handler.Config != nil {
		keyFromUrl := handler.Config.Get("gcp_destination_config.key_from_url")
		vars := mux.Vars(request)
		creds := vars["creds"]
		if keyFromUrl != nil && keyFromUrl == true && creds != "" {
			return handler.GetConnection(creds)
		}
	}
	return handler.GetConnection("")
}

func (handler *Handler) ReturnConnection(client s3.GCPClient, request *http.Request) {
	if handler.Config != nil {
		keyFromUrl := handler.Config.Get("gcp_destination_config.key_from_url")
		vars := mux.Vars(request)
		creds := vars["creds"]
		if keyFromUrl != nil && keyFromUrl == true && creds != "" {
			handler.ReturnConnectionByKey(client, creds)
			return
		}
	}
	handler.ReturnConnectionByKey(client, "")
}

func (handler *Handler) ReturnConnectionByKey(client s3.GCPClient, key string) {
	handler.gcpClientPoolLock.Lock()
	defer handler.gcpClientPoolLock.Unlock()
	handler.GCPClientPool[key] = append(handler.GCPClientPool[key], client)
}


func NewHandler(config *viper.Viper) Handler {
	return Handler{
		Config: config,
		GCPClientToBucket: func(bucket string, client s3.GCPClient) s3.GCPBucket {
			return client.Bucket(bucket)
		},
		GCPBucketToObject: func(name string, bucket s3.GCPBucket) s3.GCPObject {
			return bucket.Object(name)
		},
		GCPClientPerKey: make(map[string]s3.GCPClient),
		GCPClientPool:   make(map[string][]s3.GCPClient),
		GCPObjectToWriter: func(object s3.GCPObject, ctx context.Context) s3.GCPObjectWriter {
			return object.NewWriter(ctx)
		},
	}
}

func (handler *Handler) SetGCPClientFromCreds(creds *string) (s3.GCPClient, error) {
	if connection, ok := handler.GCPClientPerKey[*creds]; ok {
		return connection, nil
	} else {
		decrypted, _ := base64.StdEncoding.DecodeString(*creds)
		// _ = handler.GetGCPClient().Close()
		handler.gcpClientMapLock.Lock()
		defer handler.gcpClientMapLock.Unlock()
		var client s3.GCPClient
		var doubleCheck bool
		var err error
		if client, doubleCheck = handler.GCPClientPerKey[*creds]; !doubleCheck {
			client, err = storage.NewClient(*handler.GetContext(), option.WithCredentialsJSON([]byte(decrypted)))
		}
		return client, err
	}
}

func (handler *Handler) Shutdown() {
	for _, pool := range handler.GCPClientPool {
		for _, conn := range pool {
			conn.Close()
		}
	}
	logging.Log.Debug("Shutdown gcs")
}
func (handler *Handler) GetContext() *context.Context {
	return handler.Context
}
func (handler *Handler) GetConfig() *viper.Viper {
	return handler.Config
}
func (handler *Handler) SetContext(context *context.Context) {
	handler.Context = context
}
func (handler *Handler) SetConfig(config *viper.Viper) {
	handler.Config = config
}
