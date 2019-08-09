#!/bin/bash
version=`go run main.go --version`
env GOOS=linux GOARCH=amd64 go build -o cloud_sidecar_${version}_amd64 main.go
env GOOS=darwin GOARCH=amd64 go build -o cloud_sidecar_${version}_darwin main.go

