package logging

import (
	"github.com/op/go-logging"
	"os"
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

