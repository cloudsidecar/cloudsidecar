package handler

import (
	"context"
	"github.com/spf13/viper"
)

type HandlerInterface interface {
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)
}
