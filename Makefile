all:
	go fmt ./...
	go vet ./...
	go test -cover ./... -race

cover:
	go test -cover -coverprofile=coverage.out -race ./...
	go tool cover -html=coverage.out
	rm -f coverage.out

build:
	go build -o gotasks
