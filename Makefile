DEPS := $(shell git ls-files '*.go' | grep -v '^vendor')

.phony: build

build: mesos-runonce

mesos-runonce: $(DEPS)
	CGO_ENABLED=0 go build -o $@ -x -a -installsuffix cgo -ldflags '-s'

run: build
	bash ./run.sh

test: minimesos
	$$HOME/.minimesos/bin/minimesos --help

minimesos:
	[ -e $$HOME/.minimesos/bin/minimesos ] || curl -sSL https://minimesos.org/install | sh
