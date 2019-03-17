package logging

import (
	"fmt"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"os"
	conf "cloudsidecar/pkg/config"
	"strings"
)

var Log = logging.MustGetLogger("cloud")

func Init(pattern string, level string) {
	if pattern == "" {
		pattern = `%{color}%{time:2006-01-02T15:04:05.999Z-07:00} %{shortfile} > %{level:.4s}%{color:reset}:  %{message}`
	}
	var format = logging.MustStringFormatter(pattern, )
	var backend = logging.NewLogBackend(os.Stdout, "", 0)
	var backendFormatter = logging.NewBackendFormatter(backend, format)
	var backendLeveled = logging.AddModuleLevel(backendFormatter)
	loggingLevel := logging.INFO
	switch strings.ToLower(level) {
	case "info":
		loggingLevel = logging.INFO
	case "debug":
		loggingLevel = logging.DEBUG
	}
	backendLeveled.SetLevel(loggingLevel, "")
	logging.SetBackend(backendLeveled)
}

func LogUsingGCP() {
	Log.Notice("Using GCP")
}

func LogUsingAWS() {
	Log.Notice("Using AWS")
}

func LoadConfig(config *conf.Config) {
	err := viper.Unmarshal(config)
	if err != nil {
		panic(fmt.Sprint("Cannot load config ", os.Args[1], err))
	}
	format, level := "", ""
	if config.Logger != nil && config.Logger.Format != nil {
		format = *config.Logger.Format
	}
	if config.Logger != nil && config.Logger.Level != nil {
		level = *config.Logger.Level
	}
	Init(format, level)
}

