package main

import (
  "fmt"
  "github.com/gorilla/mux"
  "log"
  "net/http"
  "time"
)
func main() {
  fmt.Println("hi")
  r := mux.NewRouter()
  r.HandleFunc("/test", testHandler).Methods("GET")
  srv := &http.Server{
    Handler:      r,
    Addr:         "127.0.0.1:8000",
    // Good practice: enforce timeouts for servers you create!
    WriteTimeout: 15 * time.Second,
    ReadTimeout:  15 * time.Second,
  }

  log.Fatal(srv.ListenAndServe())
}

func testHandler(writer http.ResponseWriter, request *http.Request) {
  _, err := writer.Write([]byte("test"))
  if err != nil {
  	panic(fmt.Sprint("Error %s", err))
  }
  return
}
