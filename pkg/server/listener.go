package server

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	awshandler "cloudsidecar/pkg/aws/handler"
	kinesishandler "cloudsidecar/pkg/aws/handler/kinesis"
	s3handler "cloudsidecar/pkg/aws/handler/s3"
	"cloudsidecar/pkg/aws/handler/s3/bucket"
	"cloudsidecar/pkg/aws/handler/s3/object"
	csSqs "cloudsidecar/pkg/aws/handler/sqs"
	conf "cloudsidecar/pkg/config"
	"cloudsidecar/pkg/enterprise"
	"cloudsidecar/pkg/logging"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
	"net/http"
	"plugin"
	"reflect"
	"sync"
)

type Handler interface {
	handleGet(writer http.ResponseWriter, request *http.Request)
}

func newGCPStorage(ctx context.Context, keyFileLocation string) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithCredentialsFile(keyFileLocation))
}

func newGCPStorageNoCreds(ctx context.Context) (*storage.Client, error) {
	return storage.NewClient(ctx)
}

func newGCPStorageRawKey(ctx context.Context, rawKey string) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(rawKey)))
}

func newGCPPubSub(ctx context.Context, project string, keyFileLocation string) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, project, option.WithCredentialsFile(keyFileLocation))
}

func newGCPKmsClient(ctx context.Context, keyFileLocation string) (*kms.KeyManagementClient, error) {
	return kms.NewKeyManagementClient(ctx, option.WithCredentialsFile(keyFileLocation))
}

func newGCPBigTable(ctx context.Context, project string, instance string, keyFileLocation string) (*bigtable.Client, error) {
	return bigtable.NewClient(ctx, project, instance, option.WithCredentialsFile(keyFileLocation))
}

func newGCPDatastore(ctx context.Context, project string, keyFileLocation string) (*datastore.Client, error) {
	return datastore.NewClient(ctx, project, option.WithCredentialsFile(keyFileLocation))
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logging.Log.Noticef("%s %s", r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func getMiddlewares(enterpriseSystem enterprise.Enterprise, config *conf.Config) map[string]func (http.Handler) http.Handler {
	results := make(map[string]func (http.Handler) http.Handler)
	enterpriseMiddlewares := enterpriseSystem.RegisterMiddlewares()

	for key, middleware := range config.Middleware {
		if enterpriseMiddleware, ok := enterpriseMiddlewares[middleware.Type]; ok {
			results[key] = enterpriseMiddleware(viper.Sub(fmt.Sprint("middleware.", key)))
		} else {
			plug, err := plugin.Open(fmt.Sprint("plugin/middleware/", middleware.Type, ".so"))
			if err != nil {
				logging.Log.Error("Cannot load middleware ", middleware.Type, " for ", key)
			} else {
				sym, symErr := plug.Lookup("Register")
				if symErr != nil {
					logging.Log.Error("Cannot call register from middleware", middleware.Type, " ", symErr)
				} else {
					registerFunc := sym.(func (config *viper.Viper) func(http.Handler) http.Handler)
					results[key] = registerFunc(viper.Sub(fmt.Sprint("middleware.", key)))
				}
			}
		}
	}
	return results
}

func Main(cmd *cobra.Command, args []string) {
	var config conf.Config
	var serverWaitGroup sync.WaitGroup
	var enterpriseSystem enterprise.Enterprise
	enterprise.RegisterType(reflect.TypeOf(enterprise.Noop{}))
	enterpriseSystem = enterprise.GetSingleton()
	logging.LoadConfig(&config)
	viper.WatchConfig()
	awsHandlers := make(map[string]awshandler.HandlerInterface)
	viper.OnConfigChange(func(e fsnotify.Event) {
		logging.Log.Debug("Config file changed:", e.Name)
		logging.LoadConfig(&config)
		for key, handler := range awsHandlers {
			handler.SetConfig(viper.Sub(fmt.Sprint("aws_configs.", key)))
		}
	})
	logging.Log.Info("Started... ")
	middlewares := getMiddlewares(enterpriseSystem, &config)
	for key, awsConfig := range config.AwsConfigs  {
		toListen := true
		port := awsConfig.Port
		r := mux.NewRouter()
		r.Use(loggingMiddleware)
		configs := defaults.Config()
		creds := aws.NewStaticCredentialsProvider(awsConfig.DestinationAWSConfig.AccessKeyId, awsConfig.DestinationAWSConfig.SecretAccessKey, "" )
		configs.Credentials = creds
		//configs.EndpointResolver = aws.ResolveWithEndpointURL("http://localhost:9000")
		configs.Region = endpoints.UsEast1RegionID
		ctx := context.Background()
		for _, middlewareName := range awsConfig.Middleware {
			if middleware, ok := middlewares[middlewareName]; ok {
				r.Use(middleware)
			} else {
				logging.Log.Error("Could not find middleware ", middlewareName)
			}
		}
		if awsConfig.ServiceType == "s3" {
			svc := s3.New(configs)
			handler := s3handler.Handler{
				S3Client: svc,
				Config: viper.Sub(fmt.Sprint("aws_configs.", key)),
				GCPClientToBucket: func(bucket string, client s3handler.GCPClient) s3handler.GCPBucket {
					return client.Bucket(bucket)
				},
				GCPBucketToObject: func(name string, bucket s3handler.GCPBucket) s3handler.GCPObject {
					return bucket.Object(name)
				},
				GCPClientPerKey: make(map[string]s3handler.GCPClient),
			}
			if awsConfig.DestinationGCPConfig != nil {
				var gcpClient *storage.Client
				var err error
				if awsConfig.DestinationGCPConfig.KeyFileLocation != nil{
					gcpClient, err = newGCPStorage(ctx, *awsConfig.DestinationGCPConfig.KeyFileLocation)
				} else if awsConfig.DestinationGCPConfig.KeyFromUrl != nil && *awsConfig.DestinationGCPConfig.KeyFromUrl {
					gcpClient, err = newGCPStorageNoCreds(ctx)
				} else {
					gcpClient, err = newGCPStorageRawKey(ctx, *awsConfig.DestinationGCPConfig.RawKey)
				}
				if err != nil {
					panic(fmt.Sprintln("Error setting up gcp client", err))
				}
				handler.GCPClient = gcpClient
				handler.Context = &ctx
			}
			bucketHandler := bucket.New(&handler)
			objectHandler := object.New(&handler)
			awsHandlers[key] = &handler
			bucketHandler.Register(r)
			objectHandler.Register(r)
		} else if awsConfig.ServiceType == "kinesis" {
			svc := kinesis.New(configs)
			handler := kinesishandler.Handler{
				KinesisClient: svc,
				Config:        viper.Sub(fmt.Sprint("aws_configs.", key)),
				GCPClientToTopic: func(topic string, client kinesishandler.GCPClient) kinesishandler.GCPTopic {
					return client.Topic(topic)
				},
				GCPResultWrapper: func(result *pubsub.PublishResult) kinesishandler.GCPPublishResult {
					return result
				},
			}
			if awsConfig.DestinationGCPConfig != nil {
				gcpClient, err := newGCPPubSub(
					ctx,
					awsConfig.DestinationGCPConfig.Project,
					*awsConfig.DestinationGCPConfig.KeyFileLocation,
				)
				if err != nil {
					panic(fmt.Sprintln("Error setting up gcp client", err))
				}
				gcpKmsClient, err := newGCPKmsClient(ctx, *awsConfig.DestinationGCPConfig.KeyFileLocation)
				if err != nil {
					panic(fmt.Sprintln("Error setting up gcp client", err))
				}
				handler.GCPClient = gcpClient
				handler.GCPKMSClient = gcpKmsClient
				handler.Context = &ctx
			}
			awsHandlers[key] = &handler
			wrappedHandler := kinesishandler.New(&handler)
			wrappedHandler.Register(r)
		} else if awsConfig.ServiceType == "sqs" {
			svc := sqs.New(configs)
			handler := csSqs.Handler{
				SqsClient: svc,
				Config:        viper.Sub(fmt.Sprint("aws_configs.", key)),
				GCPClientToTopic: func(topic string, client kinesishandler.GCPClient) kinesishandler.GCPTopic {
					return client.Topic(topic)
				},
				GCPResultWrapper: func(result *pubsub.PublishResult) kinesishandler.GCPPublishResult {
					return result
				},
			}
			awsHandlers[key] = &handler
			handler.Register(r)
		} else if awsConfig.ServiceType == "" {
			logging.Log.Error("No service type configured for port ", awsConfig.Port)
		} else if enterpriseSystem.RegisterHandler(awsConfig, r, serverWaitGroup){
			toListen = false
			// do nothing, enterprise got this
		} else {
			plug, err := plugin.Open(fmt.Sprint("plugin/handler/", awsConfig.ServiceType, ".so"))
			if err != nil {
				logging.Log.Error("Cannot load plugin ", awsConfig.ServiceType, " for port ", awsConfig.Port, err)
			} else {
				sym, symErr := plug.Lookup("Register")
				if symErr != nil {
					logging.Log.Error("Cannot call register from plugin", awsConfig.ServiceType, " ", symErr)
				} else {
					registerFunc := sym.(func(*mux.Router) awshandler.HandlerInterface)
					handler := registerFunc(r)
					handler.SetConfig(viper.Sub(fmt.Sprint("aws_configs.", key)))
					handler.SetContext(&ctx)
					awsHandlers[key] = handler.(awshandler.HandlerInterface)
				}
			}
		}
		r.PathPrefix("/").HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			logging.Log.Info("Catch all %s %s %s", request.URL, request.Method, request.Header)
			writer.WriteHeader(404)
		})
		serverWaitGroup.Add(1)
		if toListen {
			srv := &http.Server{
				Handler: r,
				Addr:    fmt.Sprintf("127.0.0.1:%d", port),
			}
			go func(){
				logging.Log.Error("", srv.ListenAndServe())
				serverWaitGroup.Done()
			}()
		}
	}
	serverWaitGroup.Wait()
}
