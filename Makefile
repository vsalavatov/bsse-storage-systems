all:
	go build cmd/server.go

protoc-plugin:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc:
	mkdir -p protocol
	protoc -I=. --go_out=. kv.proto

.PHONY: all protoc protoc-plugin
