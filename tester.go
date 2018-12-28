package main

import (
	"net/http"
)


func HandleGet(writer http.ResponseWriter, request *http.Request) {
	_, err := writer.Write([]byte("moops"))
	if err != nil {
		panic(err)
	}
}
