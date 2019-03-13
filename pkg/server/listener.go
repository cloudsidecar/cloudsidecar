package server

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
	"log"
	"net/http"
	awshandler "sidecar/pkg/aws/handler"
	dynamohandler "sidecar/pkg/aws/handler/dynamo"
	kinesishandler "sidecar/pkg/aws/handler/kinesis"
	s3handler "sidecar/pkg/aws/handler/s3"
	"sidecar/pkg/aws/handler/s3/bucket"
	"sidecar/pkg/aws/handler/s3/object"
	conf "sidecar/pkg/config"
	"sidecar/pkg/logging"
	"time"
)

type Handler interface {
	handleGet(writer http.ResponseWriter, request *http.Request)
}

func newGCPStorage(ctx context.Context, keyFileLocation string) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithCredentialsFile(keyFileLocation))
}

func newGCPStorageRawKey(ctx context.Context, rawKey string) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(rawKey)))
}

func newGCPPubSub(ctx context.Context, project string, keyFileLocation string) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, project, option.WithCredentialsFile(keyFileLocation))
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

func Main(cmd *cobra.Command, args []string) {
	var config conf.Config
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
	for key, awsConfig := range config.AwsConfigs  {
		port := awsConfig.Port
		r := mux.NewRouter()
		r.Use(loggingMiddleware)
		configs := defaults.Config()
		creds := aws.NewStaticCredentialsProvider(awsConfig.DestinationAWSConfig.AccessKeyId, awsConfig.DestinationAWSConfig.SecretAccessKey, "" )
		configs.Credentials = creds
		//configs.EndpointResolver = aws.ResolveWithEndpointURL("http://localhost:9000")
		configs.Region = endpoints.UsEast1RegionID
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
			}
			if awsConfig.DestinationGCPConfig != nil {
				ctx := context.Background()
				var gcpClient *storage.Client
				var err error
				if awsConfig.DestinationGCPConfig.KeyFileLocation != nil{
					gcpClient, err = newGCPStorage(ctx, *awsConfig.DestinationGCPConfig.KeyFileLocation)
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
				Config: viper.Sub(fmt.Sprint("aws_configs.", key)),
			}
			if awsConfig.DestinationGCPConfig != nil {
				ctx := context.Background()
				gcpClient, err := newGCPPubSub(
					ctx,
					awsConfig.DestinationGCPConfig.Project,
					*awsConfig.DestinationGCPConfig.KeyFileLocation,
				)
				if err != nil {
					panic(fmt.Sprintln("Error setting up gcp client", err))
				}
				handler.GCPClient = gcpClient
				handler.Context = &ctx
			}
			awsHandlers[key] = &handler
			wrappedHandler := kinesishandler.New(&handler)
			wrappedHandler.Register(r)
		} else if awsConfig.ServiceType == "dynamo" {
			svc := dynamodb.New(configs)
			handler := dynamohandler.Handler{
				DynamoClient: svc,
				Config: viper.Sub(fmt.Sprint("aws_configs.", key)),
			}
			if awsConfig.DestinationGCPConfig != nil {
				ctx := context.Background()
				if awsConfig.DestinationGCPConfig.IsBigTable{
					gcpClient, err := newGCPBigTable(
						ctx,
						awsConfig.DestinationGCPConfig.Project,
						awsConfig.DestinationGCPConfig.Instance,
						*awsConfig.DestinationGCPConfig.KeyFileLocation,
					)
					if err != nil {
						panic(fmt.Sprintln("Error setting up gcp client", err))
					}
					handler.GCPBigTableClient = gcpClient
				} else if awsConfig.DestinationGCPConfig.DatastoreConfig != nil {
					gcpClient, err := newGCPDatastore(
						ctx,
						awsConfig.DestinationGCPConfig.Project,
						*awsConfig.DestinationGCPConfig.KeyFileLocation,
					)
					if err != nil {
						panic(fmt.Sprintln("Error setting up gcp client", err))
					}
					handler.GCPDatastoreClient = gcpClient
				}
				handler.Context = &ctx
			}
			awsHandlers[key] = &handler
			handlerWrapper := dynamohandler.New(&handler)
			handlerWrapper.Register(r)
		}
		r.PathPrefix("/").HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			logging.Log.Info("Catch all %s %s %s", request.URL, request.Method, request.Header)
			writer.WriteHeader(404)
		})
		srv := &http.Server{
			Handler: r,
			Addr:    fmt.Sprintf("127.0.0.1:%d", port),
			// Good practice: enforce timeouts for servers you create!
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		}
		go func(){
			log.Fatal(srv.ListenAndServe())
		}()
	}
	r := mux.NewRouter()
	/*
	plug, err := plugin.Open("eng.so")
	if err != nil {
		panic(err)
	}
	sym, err := plug.Lookup("HandleGet")
	if err != nil {
		panic(err)
	}
	r.HandleFunc("/sym", sym.(func(http.ResponseWriter, *http.Request))).Methods("GET")
	*/
	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8000",
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
