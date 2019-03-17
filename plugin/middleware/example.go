package main

import (
	"github.com/spf13/viper"
	"net/http"
)

func Register(config *viper.Viper) func (next http.Handler) http.Handler {
	return func (next http.Handler) http.Handler{
		return http.HandlerFunc(func (w http.ResponseWriter, r *http.Request){
			println("Something something ", config.GetInt("port"))
			next.ServeHTTP(w, r)
		})
	}
}
