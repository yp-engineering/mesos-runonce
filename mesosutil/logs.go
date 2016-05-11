package mesosutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

func FetchLog(logUrl string) ([]byte, error) {
	resp, err := defaultClient.Get(logUrl)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, ProxyError{resp.Status, resp.StatusCode}
	}

	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

type ProxyError struct {
	Status     string
	StatusCode int
}

func (p ProxyError) Error() string {
	return p.Status
}

type MesosState struct {
	CompletedFrameworks []Framework `json:"completed_frameworks"`
}

func (m MesosState) Directory(frameworkId string) string {
	for _, f := range m.CompletedFrameworks {
		// should we check for the framework?
		for _, e := range f.CompletedExecutors {
			for _, t := range e.CompletedTasks {
				if t.FrameworkId == frameworkId {
					log.V(2).Infoln("Matching task id " + frameworkId)
					return e.Directory
				}
			}
		}
	}
	return ""
}

type Tasks struct {
	FrameworkId string `json:"framework_id"`
}

type Framework struct {
	Name               string
	CompletedExecutors []Executor `json:"completed_executors"`
}

type Executor struct {
	CompletedTasks []Tasks `json:"completed_tasks"`
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

func FetchLogs(status *mesos.TaskStatus, offset int, file string, frameworkId string) ([]byte, error) {
	var (
		dir      string
		hostname string
		err      error
	)
	switch status.GetState() {
	case mesos.TaskState_TASK_FAILED,
		mesos.TaskState_TASK_KILLED:
		hostname = *status.ContainerStatus.NetworkInfos[0].IpAddress
		url := "http://" + hostname + ":5051/state.json"
		resp, err := defaultClient.Get(url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 {
			return nil, ProxyError{resp.Status, resp.StatusCode}
		}
		defer resp.Body.Close()
		var ms MesosState
		err = json.NewDecoder(resp.Body).Decode(&ms)
		if err != nil {
			return nil, err
		}
		dir = ms.Directory(frameworkId)
	default:
		var mtsd MesosTaskStatusData
		err = json.Unmarshal(status.Data, &mtsd)
		if err != nil {
			return nil, err
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
			}
		}
		domainName := ""
		if firstMtsd.Config.Domainname != "" {
			domainName = "." + firstMtsd.Config.Domainname
		}
		hostname = firstMtsd.Config.Hostname + domainName
	}
	logUrl := fmt.Sprintf("http://%s:5051/files/read.json?path=%s/%s&offset=%d",
		hostname, dir, file, offset)
	bodyData, _ := FetchLog(logUrl)

	var logData LogData
	err = json.Unmarshal(bodyData, &logData)
	if err != nil {
		return nil, err
	}
	return []byte(logData.Data), nil
}
