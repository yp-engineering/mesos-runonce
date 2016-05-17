package mesosutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type ProxyError struct {
	Status     string
	StatusCode int
}

func (p *ProxyError) Error() string {
	return p.Status
}

type DirNotFound struct {
	MesosTaskStatusData MesosTaskStatusData
}

func (d *DirNotFound) Error() string {
	return fmt.Sprintf("Directory not found for task status of: %#v", d.MesosTaskStatusData)
}

type MesosState struct {
	CompletedFrameworks []Framework `json:"completed_frameworks"`
	Frameworks          []Framework
}

type Tasks struct {
	FrameworkId string `json:"framework_id"`
}

type Framework struct {
	Name               string
	CompletedExecutors []Executor `json:"completed_executors"`
	Executors          []Executor
}

type Executor struct {
	CompletedTasks []Tasks `json:"completed_tasks"`
	Tasks          []Tasks
	Source         string
	Directory      string
}

type MesosTaskStatusMounts []struct {
	Source string `json:"Source"`
}

type MesosTaskStatusConfig struct {
	Hostname   string `json:"Hostname"`
	Domainname string `json:"Domainname"`
}

type MesosTaskStatusData []struct {
	Mounts MesosTaskStatusMounts `json:"Mounts"`
	Config MesosTaskStatusConfig `json:"Config"`
}

type LogData struct {
	Data   string `json:"data"`
	Offset int    `json:"offset"`
}

type HostDir struct {
	Host string
	Dir  string
}

func (m MesosState) Directory(frameworkId string) string {
	for _, f := range append(m.CompletedFrameworks, m.Frameworks...) {
		// should we check for the framework?
		for _, e := range append(f.CompletedExecutors, f.Executors...) {
			for _, t := range append(e.CompletedTasks, e.Tasks...) {
				if t.FrameworkId == frameworkId {
					log.V(2).Infoln("Matching task id " + frameworkId)
					return e.Directory
				}
			}
		}
	}
	return ""
}

func fetchUrl(url string) ([]byte, error) {
	resp, err := defaultClient.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, &ProxyError{resp.Status, resp.StatusCode}
	}

	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func hostDirFromState(status *mesos.TaskStatus, frameworkId string) (HostDir, error) {
	hostname := *status.ContainerStatus.NetworkInfos[0].IpAddress
	bodyData, err := fetchUrl("http://" + hostname + ":5051/state.json")
	if err != nil {
		return HostDir{}, err
	}

	var ms MesosState
	err = json.Unmarshal(bodyData, &ms)
	if err != nil {
		return HostDir{}, err
	}

	dir := ms.Directory(frameworkId)
	hostDir := HostDir{
		Host: hostname,
		Dir:  dir,
	}

	return hostDir, err
}

func hostDirFromTaskStatus(status *mesos.TaskStatus) (HostDir, error) {
	var (
		dir  string
		mtsd MesosTaskStatusData
	)
	err := json.Unmarshal(status.Data, &mtsd)
	if err != nil {
		return HostDir{}, err
	}
	// status.Data is an array of one value :( Maybe there is a better way to marshal it?
	firstMtsd := mtsd[0]
	log.V(2).Infof("firstMtsd: %#v", firstMtsd)
	for _, mount := range firstMtsd.Mounts {
		source := mount.Source
		log.V(2).Infoln("mount: ", source)
		matched, _ := regexp.MatchString("slaves.*frameworks.*executors", source)
		if matched {
			dir = source
			break
		}
	}

	if dir == "" {
		return HostDir{}, &DirNotFound{MesosTaskStatusData: mtsd}
	}

	domainName := ""
	if firstMtsd.Config.Domainname != "" {
		domainName = "." + firstMtsd.Config.Domainname
	}
	hostname := firstMtsd.Config.Hostname + domainName

	hostDir := HostDir{
		Host: hostname,
		Dir:  dir,
	}

	return hostDir, err
}

func FetchLogs(status *mesos.TaskStatus, offset int, file string, frameworkId string) ([]byte, error) {
	var (
		dir      string
		hostname string
		err      error
	)
	switch status.GetState() {
	case mesos.TaskState_TASK_FAILED,
		mesos.TaskState_TASK_KILLED:
		hostDir, err := hostDirFromState(status, frameworkId)
		if err != nil {
			return nil, err
		}
		hostname, dir = hostDir.Host, hostDir.Dir
	default:
		hostDir, err := hostDirFromTaskStatus(status)
		if err != nil {
			return nil, err
		}
		hostname, dir = hostDir.Host, hostDir.Dir

	}
	url := fmt.Sprintf("http://%s:5051/files/read.json?path=%s/%s&offset=%d",
		hostname, dir, file, offset)
	bodyData, err := fetchUrl(url)
	if err != nil {
		return nil, err
	}

	var logData LogData
	err = json.Unmarshal(bodyData, &logData)
	if err != nil {
		return nil, err
	}
	return []byte(logData.Data), nil
}
