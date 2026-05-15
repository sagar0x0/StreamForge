.PHONY: proto build test bench clean

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/*.proto

build:
	go build -o bin/broker ./cmd/broker
	go build -o bin/processor ./cmd/processor
	go build -o bin/loadgen ./cmd/loadgen

test:
	go test -v ./...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/
