package enterprise

import (
	"cloudsidecar/pkg/config"
	"github.com/gorilla/mux"
	"net/http"
	"reflect"
	"sync"
)

var instance Enterprise
var mutex sync.Mutex
var instanceType reflect.Type

type Enterprise interface {
	RegisterHandler(awsConfig *config.AWSConfig, router *mux.Router) bool
	RegisterMiddlewares() map[string]func (next http.Handler) http.Handler
	Init()
}

func RegisterType(typ reflect.Type) {
	instanceType = typ
}


func GetSingleton() Enterprise {
	if instance != nil {
		return instance
	} else {
		mutex.Lock()
		defer mutex.Unlock()
		if instance != nil {
			return instance
		} else {
			if instanceType != nil {
				//newItem := reflect.New(reflect.PtrTo(instanceType)).Interface().(Enterprise)
				newItem := reflect.New(instanceType).Interface().(Enterprise)
				instance = newItem
			} else {
				instance = &Noop{}
			}
			instance.Init()
			return instance
		}
	}
}

type Noop struct {

}

func (*Noop) RegisterHandler(awsConfig *config.AWSConfig, router *mux.Router) bool {
	return false
}

func (*Noop) RegisterMiddlewares() map[string]func (next http.Handler) http.Handler {
	return make(map[string]func (next http.Handler) http.Handler)
}

func (*Noop) Init() {

}
