.PHONY: all
all: install build

.PHONY: build
build: clean
	go build -o dist/ec-usage-analyzer pkg/cli/main.go

.PHONY: vendor
install:
	go mod tidy
	go mod vendor

.PHONY: clean
clean:
	rm -rf dist
