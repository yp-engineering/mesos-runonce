.phony: build

build: mesos-runonce

mesos-runonce: main.go
	CGO_ENABLED=0 go build -o $@ -x -a -installsuffix cgo -ldflags '-s' $<

run: build
	bash ./run.sh
