package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"plugin"
	conf "sidecar/config"
	myhandler "sidecar/handler"
	"time"
)

type Handler interface {
	handleGet(writer http.ResponseWriter, request *http.Request)
}

func main() {
	config := conf.FromFile(os.Args[1])
	fmt.Println("hi ", config)
	for _, awsConfig := range config.AwsConfigs  {
		port := awsConfig.Port
		r := mux.NewRouter()
		configs := defaults.Config()
		creds := aws.NewStaticCredentialsProvider(awsConfig.DestinationAWSConfig.AccessKeyId, awsConfig.DestinationAWSConfig.SecretAccessKey, "" )
		configs.Credentials = creds
		//configs.EndpointResolver = aws.ResolveWithEndpointURL("http://localhost:9000")
		configs.Region = endpoints.UsEast1RegionID
		svc := s3.New(configs)
		s3Handler := myhandler.S3Handler{S3Client: svc}
		r.HandleFunc("/{bucket}", s3Handler.S3ACL).Queries("acl", "").Methods("GET")
		r.HandleFunc("/{bucket}/", s3Handler.S3ACL).Queries("acl", "").Methods("GET")
		r.HandleFunc("/{bucket}", s3Handler.S3List).Methods("GET")
		r.HandleFunc("/{bucket}/", s3Handler.S3List).Methods("GET")
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
	s3 := myhandler.S3Handler{S3Client: nil}
	r.HandleFunc("/test", s3.S3List).Methods("GET")
	plug, err := plugin.Open("eng.so")
	if err != nil {
		panic(err)
	}
	sym, err := plug.Lookup("HandleGet")
	if err != nil {
		panic(err)
	}
	r.HandleFunc("/boo", func(writer http.ResponseWriter, request *http.Request) {
		writer.Write([]byte("HAR "))
	}).Methods("GET")
	r.HandleFunc("/sym", sym.(func(http.ResponseWriter, *http.Request))).Methods("GET")
	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8000",
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
