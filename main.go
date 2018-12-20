package main

import (
  "fmt"
  "github.com/gorilla/mux"
  "log"
  "net/http"
  "sidecar/handler"
  "time"
)
func main() {
  fmt.Println("hi")
  r := mux.NewRouter()
  s3 := handler.S3Handler{Client: nil}
  r.HandleFunc("/test", s3.S3List).Methods("GET")
  srv := &http.Server{
    Handler:      r,
    Addr:         "127.0.0.1:8000",
    // Good practice: enforce timeouts for servers you create!
    WriteTimeout: 15 * time.Second,
    ReadTimeout:  15 * time.Second,
  }

  log.Fatal(srv.ListenAndServe())
}

