DEPS := $(shell git ls-files '*.go' | grep -v '^vendor')

.phony: build

build: mesos-runonce

mesos-runonce: $(DEPS)
	CGO_ENABLED=0 go build -o $@ -x -a -installsuffix cgo -ldflags '-s'

run: build
	bash ./run.sh
