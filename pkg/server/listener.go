package server

import (
	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	awshandler "cloudsidecar/pkg/aws/handler"
	gcpHandler "cloudsidecar/pkg/gcp/handler"
	conf "cloudsidecar/pkg/config"
	"cloudsidecar/pkg/enterprise"
	"cloudsidecar/pkg/logging"
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
	googleHttp "google.golang.org/api/transport/http"
	"net"
	"net/http"
	"plugin"
	"reflect"
	"sync"
	"time"
)

const Version = "0.0.16"

// Handlers map
var awsHandlers map[string]awshandler.HandlerInterface
var gcpHandlers map[string]gcpHandler.HandlerInterface

// Handles http requests
var routes map[string]*RouteWrapper

// Http Servers map
var awsServers map[string]*http.Server
var gcpServers map[string]*http.Server

// Creates an http client for GCP.  This is needed so we can set timeouts and not
// share http/2 connections between gcp uses, which helps with GCS
func httpClientForGCP(ctx context.Context, opts ...option.ClientOption) *http.Client {
	roundTrip := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1,
		IdleConnTimeout:       5 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   1,
		MaxConnsPerHost:       1,
	}
	// This is from the SDK
	userAgent := "gcloud-golang-storage/20151204"
	o := []option.ClientOption{
		option.WithScopes(storage.ScopeFullControl),
		option.WithUserAgent(userAgent),
	}
	opts = append(o, opts...)
	googleClient, _ := googleHttp.NewTransport(ctx, roundTrip, opts...)
	return &http.Client{
		Transport: googleClient,
	}
}

// GCS client based on keyfile
func newGCPStorage(ctx context.Context, keyFileLocation string) (*storage.Client, error) {
	client := httpClientForGCP(ctx, option.WithCredentialsFile(keyFileLocation))
	return storage.NewClient(ctx, option.WithCredentialsFile(keyFileLocation), option.WithHTTPClient(client))
}

// GCS client without creds.  Those will be added later
func newGCPStorageNoCreds(ctx context.Context) (*storage.Client, error) {
	client := httpClientForGCP(ctx)
	return storage.NewClient(ctx, option.WithHTTPClient(client))
}

// GCS client with raw key - string that has contents of key file
func newGCPStorageRawKey(ctx context.Context, rawKey string) (*storage.Client, error) {
	client := httpClientForGCP(ctx, option.WithCredentialsJSON([]byte(rawKey)))
	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(rawKey)), option.WithHTTPClient(client))
}

// PubSub client
func newGCPPubSub(ctx context.Context, project string, keyFileLocation string) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, project, option.WithCredentialsFile(keyFileLocation))
}

// KMS Client (for encryption / decryption)
func newGCPKmsClient(ctx context.Context, keyFileLocation string) (*kms.KeyManagementClient, error) {
	return kms.NewKeyManagementClient(ctx, option.WithCredentialsFile(keyFileLocation))
}

// Simple logging middleware for access log
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logging.Log.Noticef("%s %s", r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

// Gets all middlewares configured.  Looks in plugin/middleware/ for so files
func getMiddlewares(enterpriseSystem enterprise.Enterprise, config *conf.Config) map[string]func(http.Handler) http.Handler {
	results := make(map[string]func(http.Handler) http.Handler)
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
					registerFunc := sym.(func(config *viper.Viper) func(http.Handler) http.Handler)
					results[key] = registerFunc(viper.Sub(fmt.Sprint("middleware.", key)))
				}
			}
		}
	}
	return results
}

// Main function, runs http server
func Main(config *conf.Config, onConfigChange <-chan string, cmd *cobra.Command, args []string) {
	var serverWaitGroup sync.WaitGroup
	var enterpriseSystem enterprise.Enterprise
	awsHandlers = make(map[string]awshandler.HandlerInterface)
	gcpHandlers = make(map[string]gcpHandler.HandlerInterface)
	awsServers = make(map[string]*http.Server)
	gcpServers = make(map[string]*http.Server)
	routes = make(map[string]*RouteWrapper)
	enterprise.RegisterType(reflect.TypeOf(enterprise.Noop{}))
	enterpriseSystem = enterprise.GetSingleton()
	// set up logger and config reloader
	logging.LoadConfig(config)
	go func() {
		for {
			select {
			case e := <-onConfigChange:
				config = &conf.Config{}
				logging.Log.Debug("Config file changed:", e)
				logging.LoadConfig(config)
				logging.Log.Debug("Config", config)
				Listen(config, &serverWaitGroup, enterpriseSystem)
			}
		}
	}()
	Listen(config, &serverWaitGroup, enterpriseSystem)
	logging.Log.Infof("Started %s.. ", Version)
	serverWaitGroup.Wait()
}
