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
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	mlog "github.com/yp-engineering/mesos-runonce/mesosutil"
	"golang.org/x/net/context"
)

var eventCh = make(chan *mesos.TaskStatus)
var _frameworkId string
var exitStatus = 0
var config *Config

type MesosRunonceScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
}

// ----------------------- Mesos interface ------------------------- //

func (sched *MesosRunonceScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.V(1).Infoln("Framework Registered with Master ", masterInfo)
	_frameworkId = frameworkId.GetValue()
	fmt.Println("Registered with master and given framework ID:", _frameworkId)
}
func (sched *MesosRunonceScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.V(1).Infoln("Framework Re-Registered with Master ", masterInfo)
}
func (sched *MesosRunonceScheduler) Disconnected(sched.SchedulerDriver) {
	log.Exitf("disconnected from master, aborting")
}
func (sched *MesosRunonceScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.V(1).Infof("offer rescinded: %v", oid)
}
func (sched *MesosRunonceScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (sched *MesosRunonceScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.V(1).Infof("slave lost: %v", sid)
}
func (sched *MesosRunonceScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (sched *MesosRunonceScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Exitf("Scheduler received error: %v", err)
}

func (sched *MesosRunonceScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {

	if sched.tasksLaunched >= sched.totalTasks {
		log.V(1).Info("decline all of the offers since all of our tasks are already launched")
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

		log.V(1).Infoln("Received Offer <", offer.Id.GetValue(), "> on host ", *offer.Hostname, "with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		dCpus := config.Task.Docker.Cpus
		dMem := config.Task.Docker.Mem

		var tasks []*mesos.TaskInfo
		for sched.tasksLaunched < sched.totalTasks &&
			dCpus <= remainingCpus &&
			dMem <= remainingMems {

			sched.tasksLaunched++

			tID := strconv.Itoa(sched.tasksLaunched)
			if config.Task.Id != "" {
				tID = config.Task.Id
			}

			tName := "mesos-runonce-" + tID
			if config.Task.Name != "" {
				tName = config.Task.Name
			}

			taskId := &mesos.TaskID{
				Value: proto.String(tID),
			}

			containerType := mesos.ContainerInfo_DOCKER
			task := &mesos.TaskInfo{
				Name:    proto.String(tName),
				TaskId:  taskId,
				SlaveId: offer.SlaveId,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", dCpus),
					util.NewScalarResource("mem", dMem),
				},
				Command: &mesos.CommandInfo{
					Shell: proto.Bool(false),
				},
				Container: &mesos.ContainerInfo{
					Type: &containerType,
					Docker: &mesos.ContainerInfo_DockerInfo{
						Image:          proto.String(config.Task.Docker.Image),
						ForcePullImage: proto.Bool(config.Task.Docker.ForcePullImage),
					},
				},
			}
			fmt.Printf("Prepared task: [%s] with offer [%s] for launch on host [%s]\n", task.GetName(), offer.Id.GetValue(), *offer.Hostname)

			if len(config.Task.Docker.Env) != 0 || config.Task.Docker.EnvString != "" {
				task.Command.Environment = config.EnvVars()
			}

			// Allow arbitrary commands, else just use whatever the image defines in CMD
			if config.Task.Docker.Cmd != "" {
				task.Command.Value = proto.String(config.Task.Docker.Cmd)
				task.Command.Shell = proto.Bool(true)
			}

			tasks = append(tasks, task)
			remainingCpus -= dCpus
			remainingMems -= dMem
		}
		log.V(1).Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(5)})
	}
}

func (sched *MesosRunonceScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.V(1).Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	eventCh <- status

	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.V(1).Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED ||
		status.GetState() == mesos.TaskState_TASK_ERROR {
		exitStatus = 1
		log.Warningf("mesos TaskStatus: %v", status)
		driver.Stop(false)
		log.Errorln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message.", status.GetMessage(),
		)
	}
}

// ----------------------- Helper methods ------------------------- //

func printLogs() {
	timer := time.Tick(500 * time.Millisecond)
	var (
		readableStatus *mesos.TaskStatus
		finished       bool
		oout, oerr     int
	)
	for {
		select {
		case status := <-eventCh:
			switch status.GetState() {
			case mesos.TaskState_TASK_RUNNING,
				mesos.TaskState_TASK_STARTING:
				readableStatus = status
			case mesos.TaskState_TASK_FAILED,
				mesos.TaskState_TASK_LOST,
				mesos.TaskState_TASK_KILLED:
				readableStatus = status
				finished = true
			case mesos.TaskState_TASK_FINISHED:
				finished = true
			}
		case <-timer:
			if readableStatus != nil {
				if finished {
					time.Sleep(3 * time.Second)
				}
				x := printLog(readableStatus, oout, os.Stdout)
				y := printLog(readableStatus, oerr, os.Stderr)
				if finished && x == 0 && y == 0 {
					log.V(1).Infof("framework terminating")
					os.Exit(exitStatus)
				}
				oout += x
				oerr += y
			}
		}
	}
}

func printLog(status *mesos.TaskStatus, offset int, w io.Writer) int {
	file := "stdout"
	if w == os.Stderr {
		file = "stderr"
	}
	data, err := mlog.FetchLogs(status, offset, file, _frameworkId)
	if err != nil {
		log.V(1).Infof("fetch logs err: %s\n", err)
		return 0
	}
	if len(data) > 0 {
		fmt.Fprint(w, string(data))
	}
	return len(data)
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

func newMesosRunonceScheduler(exec *mesos.ExecutorInfo) *MesosRunonceScheduler {
	return &MesosRunonceScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    config.Runonce.TaskCount,
	}
}

func fwinfo() *mesos.FrameworkInfo {
	return &mesos.FrameworkInfo{
		User: proto.String(config.Runonce.MesosRunasUser),
		Name: proto.String("mesos-runonce"),
		Role: proto.String(config.Runonce.MesosRole),
	}
}

func cred(fwinfo *mesos.FrameworkInfo) *mesos.Credential {
	cred := (*mesos.Credential)(nil)
	mesosAuthPrincipal := config.Runonce.AuthPrincipal
	if mesosAuthPrincipal != "" {
		fwinfo.Principal = proto.String(mesosAuthPrincipal)
		cred = &mesos.Credential{
			Principal: proto.String(mesosAuthPrincipal),
		}
		mesosAuthSecretFile := config.Runonce.AuthSecretFile
		if mesosAuthSecretFile != "" {
			_, err := os.Stat(mesosAuthSecretFile)
			if err != nil {
				log.Exit("missing secret file: ", err.Error())
			}
			secret, err := ioutil.ReadFile(mesosAuthSecretFile)
			if err != nil {
				log.Exit("failed to read secret file: ", err.Error())
			}
			cred.Secret = proto.String(strings.TrimSuffix(string(secret), "\n"))
		}
	}
	return cred
}

func driverConfig(fwinfo *mesos.FrameworkInfo) sched.DriverConfig {
	// build command executor
	exec := prepareExecutorInfo()

	publishedAddress := parseIP(config.Runonce.Address)
	dConfig := sched.DriverConfig{
		Scheduler:        newMesosRunonceScheduler(exec),
		Framework:        fwinfo,
		Master:           config.Runonce.Master,
		Credential:       cred(fwinfo),
		PublishedAddress: publishedAddress,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, config.Runonce.AuthProvider)
			ctx = sasl.WithBindingAddress(ctx, publishedAddress)
			return ctx
		},
	}

	// Allow listening port to be configurable so we can run this inside of
	// mesos if desired.
	// NOTE only affects main PID, meaning the authentication step uses
	// another PID which picks a random (32K range) port :(. Opened
	// https://github.com/mesos/mesos-go/issues/229 to discuss.
	if config.Runonce.BindingPort != 0 {
		dConfig.BindingPort = uint16(config.Runonce.BindingPort)
	}
	return dConfig
}

// ----------------------- func main() ------------------------- //

func main() {
	log.V(1).Infoln("Initializing the Example Scheduler...")
	config = loadConfig()

	// the framework
	fwinfo := fwinfo()

	driver, err := sched.NewMesosSchedulerDriver(driverConfig(fwinfo))
	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	// Don't block on this call because we need printLogs to be the process blocker to ensure we retrieve all the logs.
	go func() {
		if stat, err := driver.Run(); err != nil {
			log.V(1).Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
		}
	}()

	printLogs()
}
