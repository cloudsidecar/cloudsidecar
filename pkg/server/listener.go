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

func newGCPPubSub(ctx context.Context, project string, keyFileLocation string) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, project, option.WithCredentialsFile(keyFileLocation))
}

func newGCPBigTable(ctx context.Context, project string, instance string, keyFileLocation string) (*bigtable.Client, error) {
	return bigtable.NewClient(ctx, project, instance, option.WithCredentialsFile(keyFileLocation))
}

func newGCPDatastore(ctx context.Context, project string, keyFileLocation string) (*datastore.Client, error) {
	return datastore.NewClient(ctx, project, option.WithCredentialsFile(keyFileLocation))
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
			}
			if awsConfig.DestinationGCPConfig != nil {
				ctx := context.Background()
				gcpClient, err := newGCPStorage(ctx, awsConfig.DestinationGCPConfig.KeyFileLocation)
				if err != nil {
					panic(fmt.Sprintln("Error setting up gcp client", err))
				}
				handler.GCPClient = gcpClient
				handler.Context = &ctx
			}
			bucketHandler := bucket.New(&handler)
			objectHandler := object.New(&handler)
			awsHandlers[key] = &handler
			r.HandleFunc("/{bucket}", bucketHandler.ACLHandle).Queries("acl", "").Methods("GET")
			r.HandleFunc("/{bucket}/", bucketHandler.ACLHandle).Queries("acl", "").Methods("GET")
			r.HandleFunc("/{bucket}", bucketHandler.ListHandle).Methods("GET")
			r.HandleFunc("/{bucket}/", bucketHandler.ListHandle).Methods("GET")
			r.HandleFunc("/{bucket}/{key:[^#?\\s]+}", objectHandler.HeadHandle).Methods("HEAD")
			r.HandleFunc("/{bucket}/{key:[^#?\\s]+}", objectHandler.GetHandle).Methods("GET")
			r.HandleFunc("/{bucket}/{key:[^#?\\s]+}", objectHandler.MultiPartHandle).Queries("uploads", "").Methods("POST")
			r.HandleFunc("/{bucket}/{key:[^#?\\s]+}", objectHandler.UploadPartHandle).Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").Methods("PUT")
			r.HandleFunc("/{bucket}/{key:[^#?\\s]+}", objectHandler.PutHandle).Methods("PUT")
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
					awsConfig.DestinationGCPConfig.KeyFileLocation,
				)
				if err != nil {
					panic(fmt.Sprintln("Error setting up gcp client", err))
				}
				handler.GCPClient = gcpClient
				handler.Context = &ctx
			}
			awsHandlers[key] = &handler
			kinesisHandler := kinesishandler.New(&handler)
			r.HandleFunc("/", kinesisHandler.PublishHandle).Methods("POST")
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
						awsConfig.DestinationGCPConfig.KeyFileLocation,
					)
					if err != nil {
						panic(fmt.Sprintln("Error setting up gcp client", err))
					}
					handler.GCPBigTableClient = gcpClient
				} else if awsConfig.DestinationGCPConfig.DatastoreConfig != nil {
					gcpClient, err := newGCPDatastore(
						ctx,
						awsConfig.DestinationGCPConfig.Project,
						awsConfig.DestinationGCPConfig.KeyFileLocation,
					)
					if err != nil {
						panic(fmt.Sprintln("Error setting up gcp client", err))
					}
					handler.GCPDatastoreClient = gcpClient
				}
				handler.Context = &ctx
			}
			awsHandlers[key] = &handler
			dynamoHandler := dynamohandler.New(&handler)
			r.HandleFunc("/", dynamoHandler.Handle).Methods("POST")
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
