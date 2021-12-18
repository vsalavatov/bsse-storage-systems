all: server client

protoc-plugin:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc:
	mkdir -p protocol
	protoc -I=. --go_out=. kv.proto

server:
	go build cmd/server/server.go

client:
	go build cmd/client/client.go

.PHONY: all protoc protoc-plugin server client
