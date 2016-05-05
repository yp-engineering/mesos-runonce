/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	mlog "github.com/yp-engineering/mesos-runonce/mesosutil"
	"golang.org/x/net/context"
)

var eventCh chan *mesos.TaskStatus

var (
	address      = flag.String("address", "127.0.0.1", "Address for mesos to callback on.")
	bindingPort  = flag.Uint("port", 0, "Port for address to use for mesos to callback.")
	authProvider = flag.String("authentication-provider", sasl.ProviderName,
		fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	master               = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	taskCount            = flag.Int("task-count", 1, "Total task count to run.")
	mesosAuthPrincipal   = flag.String("principal", "", "Mesos authentication principal.")
	mesosAuthSecretFile  = flag.String("secret-file", "", "Mesos authentication secret file.")
	mesosRunasUser       = flag.String("user", "root", "Mesos user to run tasks as.")
	dockerImage          = flag.String("docker-image", "", "Docker image to run.")
	dockerCmd            = flag.String("docker-cmd", "", "Docker command to run.")
	dockerForcePullImage = flag.Bool("force-pull", false, "Boolean for forcing pull of image before run.")
	dockerEnvVars        = flag.String("env-vars", "", "Docker env vars for the container. E.g. -env-vars='{\"env\":{\"FOO\":\"bar\"}}'")
	dCpus                = flag.Float64("cpus", 1.0, "How many CPUs to use.")
	dMem                 = flag.Float64("mem", 10, "How much memory to use.")
)

type ExampleScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
}

type DockerEnvVars struct {
	Env map[string]string `json:"env"`
}

func newExampleScheduler(exec *mesos.ExecutorInfo) *ExampleScheduler {
	return &ExampleScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    *taskCount,
	}
}

func (sched *ExampleScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}
func (sched *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}
func (sched *ExampleScheduler) Disconnected(sched.SchedulerDriver) {
	log.Exitf("disconnected from master, aborting")
}
func (sched *ExampleScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Errorf("offer rescinded: %v", oid)
}
func (sched *ExampleScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (sched *ExampleScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Errorf("slave lost: %v", sid)
}
func (sched *ExampleScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (sched *ExampleScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Exitf("Scheduler received error: %v", err)
}

func (sched *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {

	if sched.tasksLaunched >= sched.totalTasks {
		log.Info("decline all of the offers since all of our tasks are already launched")
		ids := make([]*mesos.OfferID, len(offers))
		for i, offer := range offers {
			ids[i] = offer.Id
		}
		driver.LaunchTasks(ids, []*mesos.TaskInfo{}, &mesos.Filters{RefuseSeconds: proto.Float64(120)})
		return
	}
	for _, offer := range offers {
		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		log.Infoln("Received Offer <", offer.Id.GetValue(), "> on host ", *offer.Hostname, "with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for sched.tasksLaunched < sched.totalTasks &&
			*dCpus <= remainingCpus &&
			*dMem <= remainingMems {

			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			containerType := mesos.ContainerInfo_DOCKER
			task := &mesos.TaskInfo{
				Name:    proto.String("mesos-runonce-" + taskId.GetValue()),
				TaskId:  taskId,
				SlaveId: offer.SlaveId,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", *dCpus),
					util.NewScalarResource("mem", *dMem),
				},
				Command: &mesos.CommandInfo{
					Shell: proto.Bool(false),
				},
				Container: &mesos.ContainerInfo{
					Type: &containerType,
					Docker: &mesos.ContainerInfo_DockerInfo{
						Image:          proto.String(*dockerImage),
						ForcePullImage: proto.Bool(*dockerForcePullImage),
					},
				},
			}
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			if *dockerEnvVars != "" {
				task.Command.Environment = envVars()
			}

			// Allow arbitrary commands, else just use whatever the image defines in CMD
			if *dockerCmd != "" {
				task.Command.Value = proto.String(*dockerCmd)
				task.Command.Shell = proto.Bool(true)
			}

			tasks = append(tasks, task)
			remainingCpus -= *dCpus
			remainingMems -= *dMem
		}
		log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(5)})
	}
}

func (sched *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	eventCh <- status

	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED ||
		status.GetState() == mesos.TaskState_TASK_ERROR {
		log.Warningf("mesos TaskStatus: %v", status)
		driver.Abort()
		log.Exitln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message.", status.GetMessage(),
		)
	}
}

// ----------------------- func init() ------------------------- //

func init() {
	flag.Parse()
	log.Infoln("Initializing the Example Scheduler...")
}

func fetchLogs() {
	timer := time.Tick(500 * time.Millisecond)
	finished := false
	offset := 0
	var startedStatus *mesos.TaskStatus
	var wg sync.WaitGroup
	for {
		select {
		case status := <-eventCh:
			if status.GetState() == mesos.TaskState_TASK_RUNNING ||
				status.GetState() == mesos.TaskState_TASK_STARTING ||
				status.GetState() == mesos.TaskState_TASK_FAILED ||
				status.GetState() == mesos.TaskState_TASK_KILLED {
				startedStatus = status
			}
			if status.GetState() == mesos.TaskState_TASK_FINISHED {
				finished = true
			}
		case <-timer:
			if startedStatus != nil {
				files := []string{
					"stdout",
					"stderr",
				}
				for _, file := range files {
					wg.Add(1)
					go func(file string) {
						defer wg.Done()
						data, err := mlog.FetchLogs(startedStatus, offset, file)
						if err != nil {
							log.Infof("fetch logs err: %s\n", err)
						} else if len(data) > 0 {
							switch file {
							case "stdout":
								fmt.Print(string(data))
							case "stderr":
								fmt.Fprintln(os.Stderr, (string(data)))
							}
							offset += len(data)
						} else if len(data) == 0 && finished {
							return
						}
					}(file)
				}
			}
		}
	}
	wg.Wait()
}
func prepareExecutorInfo() *mesos.ExecutorInfo {
	// Create mesos scheduler driver.
	containerType := mesos.ContainerInfo_DOCKER
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("mesos-runonce-executor"),
		Source:     proto.String("mesos-runonce-executor"),
		Container: &mesos.ContainerInfo{
			Type: &containerType,
		},
	}
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Exit(err)
	}
	if len(addr) < 1 {
		log.Exitf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}

func envVars() *mesos.Environment {
	var dEnvVars DockerEnvVars
	err := json.Unmarshal([]byte(*dockerEnvVars), &dEnvVars)
	if err != nil {
		log.Exitf("JSON error: %#v with unparsable env vars: %+v", err, *dockerEnvVars)
	}

	var variables []*mesos.Environment_Variable
	for key, value := range dEnvVars.Env {
		variables = append(variables, &mesos.Environment_Variable{Name: proto.String(key), Value: proto.String(value)})
	}

	return &mesos.Environment{Variables: variables}
}

// ----------------------- func main() ------------------------- //

func main() {

	// build command executor
	exec := prepareExecutorInfo()

	// the framework
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(*mesosRunasUser),
		Name: proto.String("mesos-runonce"),
	}

	cred := (*mesos.Credential)(nil)
	if *mesosAuthPrincipal != "" {
		fwinfo.Principal = proto.String(*mesosAuthPrincipal)
		cred = &mesos.Credential{
			Principal: proto.String(*mesosAuthPrincipal),
		}
		if *mesosAuthSecretFile != "" {
			_, err := os.Stat(*mesosAuthSecretFile)
			if err != nil {
				log.Exit("missing secret file: ", err.Error())
			}
			secret, err := ioutil.ReadFile(*mesosAuthSecretFile)
			if err != nil {
				log.Exit("failed to read secret file: ", err.Error())
			}
			cred.Secret = proto.String(string(secret))
		}
	}

	publishedAddress := parseIP(*address)
	config := sched.DriverConfig{
		Scheduler:        newExampleScheduler(exec),
		Framework:        fwinfo,
		Master:           *master,
		Credential:       cred,
		PublishedAddress: publishedAddress,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, *authProvider)
			ctx = sasl.WithBindingAddress(ctx, publishedAddress)
			return ctx
		},
	}

	// Allow listening port to be configurable so we can run this inside of
	// mesos if desired.
	if *bindingPort != 0 {
		config.BindingPort = uint16(*bindingPort)
	}

	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	eventCh = make(chan *mesos.TaskStatus)
	go fetchLogs()

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
	log.Infof("framework terminating")
}
