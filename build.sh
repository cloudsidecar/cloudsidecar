#!/bin/bash
env GOOS=linux GOARCH=amd64 go build -o cloud_sidecar_0.0.7_amd64 main.go
env GOOS=darwin GOARCH=amd64 go build -o cloud_sidecar_0.0.7_darwin main.go

