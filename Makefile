.phony: build push run

build: main.go
	CGO_ENABLED=0 go build -o mesos-runonce -x -a -installsuffix cgo -ldflags '-s' ./main.go

