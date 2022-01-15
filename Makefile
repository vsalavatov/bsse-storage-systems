all: server client loadtest

protoc-plugin:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc:
	mkdir -p protocol
	protoc -I=. --go_out=. kv.proto

server:
	go build cmd/server/server.go cmd/server/io.go cmd/server/util.go

client:
	go build cmd/client/client.go

loadtest:
	go build cmd/loadtest/loadtest.go

.PHONY: all protoc protoc-plugin server client loadtest
