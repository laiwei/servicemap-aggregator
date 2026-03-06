.PHONY: build run tidy docker clean

BINARY  := servicemap-aggregator
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-s -w -X main.Version=$(VERSION)"

build:
	go build $(LDFLAGS) -o $(BINARY) .

run: build
	./$(BINARY) \
	  --prometheus-url=http://localhost:9090 \
	  --listen=:9098 \
	  --interval=60s

tidy:
	go mod tidy

docker:
	docker build -t flashcat/servicemap-aggregator:$(VERSION) \
	  --build-arg BINARY=$(BINARY) -f Dockerfile .

clean:
	rm -f $(BINARY)
