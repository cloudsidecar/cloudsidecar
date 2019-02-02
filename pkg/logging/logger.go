package logging

import (
	"fmt"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"os"
	conf "sidecar/pkg/config"
)

var Log = logging.MustGetLogger("cloud")

func Init() {
	var format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} > %{level:.4s} %{id:03x}%{color:reset}:  %{message}`,
	)
	var backend = logging.NewLogBackend(os.Stdout, "", 0)
	var backendFormatter = logging.NewBackendFormatter(backend, format)
	var backendLeveled = logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.INFO, "")
	logging.SetBackend(backendLeveled)
}

func LoadConfig(config *conf.Config) {
	err := viper.Unmarshal(config)
	if err != nil {
		panic(fmt.Sprint("Cannot load config ", os.Args[1], err))
	}
}

