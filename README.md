# mesos-runonce
Mesos framework to run a docker container once.

# Download
Get the [latest binary](/../../releases/latest) from github releases.

# Usage
A basic working example

```bash
# Replace all of these with your values
export MESOS_PRINCIPAL=your_principal
export MESOS_PASSWORD=your_password
export MESOS_MASTER=your-master:5050 # doesn't have to be leader

./mesos-runonce -master $MESOS_MASTER \
        -address $(ip route get 8.8.8.8 | grep -o src.* | grep -oE "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+") \
        -principal $MESOS_PRINCIPAL \
        -secret-file <(printf $MESOS_PASSWORD) \
        -docker-cmd "env;sleep 20;ls" \
        -docker-image alpine
```

To see all of the command line options:

```bash
./mesos-runonce -h
```

To use a config.json file (see [example-config.json](example-config.json) for JSON spec):

```bash
./mesos-runonce -config config.json
```

To override values in the config.json:

```bash
./mesos-runonce -config config.json -principal $USER
```
