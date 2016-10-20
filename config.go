package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth/sasl"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

// See example-config.json for an example of this config.

type Docker struct {
	Image          string            `json:"image"`
	Cmd            string            `json:"cmd"`
	ForcePullImage bool              `json:"force_pull"`
	Env            map[string]string `json:"env"`
	EnvString      string            `json:"env_string,omitempty"`
	Cpus           float64           `json:"cpus"`
	Mem            float64           `json:"mem"`
}

type Runonce struct {
	Address        string `json:"address"`
	BindingPort    uint   `json:"port"`
	AuthProvider   string `json:"authentication_provider"`
	Master         string `json:"master"`
	TaskCount      int    `json:"task_count"`
	MesosRunasUser string `json:"user"`
	MesosRole      string `json:"role"`
	AuthPrincipal  string `json:"principal"`
	AuthSecretFile string `json:"secret_file"`
}

type Task struct {
	Id     string `json:"id"`
	Name   string `json:"name"`
	Docker `json:"docker"`
}

type Config struct {
	Runonce `json:"runonce"`
	Task    `json:"task"`
}

// Required interface for flag.Var
func (config *Config) Set(runonceConfig string) error {
	_, err := os.Stat(runonceConfig)
	if err != nil {
		log.Exit("missing config file: ", err.Error())
	}
	configData, err := ioutil.ReadFile(runonceConfig)
	if err != nil {
		log.Exit("failed to read config file: ", err.Error())
	}
	return json.Unmarshal([]byte(configData), config)
}

// Required interface for flag.Var. This specifies the "default" that is returned when doing -h
func (config *Config) String() string {
	return ""
}

// Used to allow passing in of string of JSON or defining in your config.json. This behaves differently
// than the other options in that -env-vars will always override what is defined in config.json
func (c *Config) EnvVars() *mesos.Environment {
	var envVars map[string]string

	if config.Task.Docker.EnvString != "" {
		var dEnvVars Docker
		err := json.Unmarshal([]byte(config.Task.Docker.EnvString), &dEnvVars)
		if err != nil {
			log.Exitf("JSON error: %#v with unparsable env vars: %+v", err, config.Task.EnvString)
		}
		envVars = dEnvVars.Env
	} else if len(config.Task.Docker.Env) != 0 {
		envVars = config.Task.Env
	}

	var variables []*mesos.Environment_Variable
	for key, value := range envVars {
		variables = append(variables, &mesos.Environment_Variable{Name: proto.String(key), Value: proto.String(value)})
	}

	return &mesos.Environment{Variables: variables}
}

// Responsible for loading up our config.json && || all the command lines switches. The way this is setup
// will be order specific on the command line. Only exception to this rule is env-vars (see EnvVars()).
//
// E.g.
//
// # address will override config.json if it is defined in config.json.
// mesos-runonce -config=config.json -address=address
//
// # config.json will override address if it is defined in config.json.
// mesos-runonce -address=address -config=config.json
func loadConfig() *Config {
	cfg := new(Config)

	flag.BoolVar(&cfg.Task.Docker.ForcePullImage, "force-pull", false, "Boolean for forcing pull of image before run.")
	flag.Float64Var(&cfg.Task.Docker.Cpus, "cpus", 1.0, "How many CPUs to use.")
	flag.Float64Var(&cfg.Task.Docker.Mem, "mem", 10, "How much memory to use.")
	flag.IntVar(&cfg.TaskCount, "task-count", 1, "Total task count to run.")
	flag.StringVar(&cfg.Runonce.Address, "address", "127.0.0.1", "Address for mesos to callback on.")
	flag.StringVar(&cfg.Runonce.AuthPrincipal, "principal", "", "Mesos authentication principal.")
	flag.StringVar(&cfg.Runonce.AuthProvider, "authentication-provider", sasl.ProviderName, fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	flag.StringVar(&cfg.Runonce.AuthSecretFile, "secret-file", "", "Mesos authentication secret file.")
	flag.StringVar(&cfg.Runonce.Master, "master", "127.0.0.1:5050", "Master address <ip:port>")
	flag.StringVar(&cfg.Runonce.MesosRunasUser, "user", "root", "Mesos user to run tasks as.")
	flag.StringVar(&cfg.Runonce.MesosRole, "role", "", "Mesos role to run tasks with.")
	flag.StringVar(&cfg.Task.Docker.Cmd, "docker-cmd", "", "Docker command to run.")
	flag.StringVar(&cfg.Task.Docker.EnvString, "env-vars", "", "Docker env vars for the container. E.g. -env-vars='{\"env\":{\"FOO\":\"bar\"}}'")
	flag.StringVar(&cfg.Task.Docker.Image, "docker-image", "", "Docker image to run.")
	flag.StringVar(&cfg.Task.Id, "task-id", "", "Mesos task id to identify the task.")
	flag.StringVar(&cfg.Task.Name, "task-name", "", "Mesos task name to label the task.")
	flag.UintVar(&cfg.Runonce.BindingPort, "port", 0, "Port for address to use for mesos to callback.")
	flag.Var(cfg, "config", "Runonce config of JSON. See spec in config.go for details.")

	flag.Parse()

	return cfg
}
