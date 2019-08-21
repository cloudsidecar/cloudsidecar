package main

import (
	"cloudsidecar/pkg/aws/handler"
	"context"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"net/http"
)

func Register(r *mux.Router) handler.HandlerInterface {
	r.HandleFunc("/moops", func(writer http.ResponseWriter, request *http.Request) {
		writer.Write([]byte("Test"))
	})
	return &Bleh{}
}

type Bleh struct {
	context *context.Context
	config  *viper.Viper
}

func (bleh *Bleh) GetContext() *context.Context {
	return bleh.context
}

func (bleh *Bleh) GetConfig() *viper.Viper {
	return bleh.config
}

func (bleh *Bleh) SetContext(context *context.Context) {
	bleh.context = context
}

func (bleh *Bleh) SetConfig(config *viper.Viper) {
	bleh.config = config
}
