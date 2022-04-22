build:
	go build -o bin/generator cmd/generator/main.go
	go build -o bin/uploader cmd/uploader/main.go