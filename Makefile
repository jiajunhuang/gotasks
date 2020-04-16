all:
	go vet ./...
	go test -cover ./...

cover:
	go test -cover -coverprofile=coverage.out -race
	go tool cover -html=coverage.out
	rm -f coverage.out

build:
	go build -o gotasks
