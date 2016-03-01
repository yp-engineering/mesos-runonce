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
	Frameworks []Framework
}

func (m MesosState) Directory(source string) string {
	for _, f := range m.Frameworks {
		// should we check for the framework?
		for _, e := range append(f.CompletedExecutors, f.Executors...) {
			if e.Source == source {
				return e.Directory
			}
		}
	}
	return ""
}

type Framework struct {
	Name               string
	CompletedExecutors []Executor `json:"completed_executors"`
	Executors          []Executor
}

type Executor struct {
	Source    string
	Directory string
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

func FetchLogs(status *mesos.TaskStatus, offset int) ([]byte, error) {
	var mtsd MesosTaskStatusData
	err := json.Unmarshal(status.Data, &mtsd)
	if err != nil {
		return nil, err
	}
	// status.Data is an array of one value :( Maybe there is a better way to marshal it?
	firstMtsd := mtsd[0]
	log.Infof("firstMtsd: %#v", firstMtsd)
	var dir string
	for _, mount := range firstMtsd.Mounts {
		source := mount.Source
		log.Infoln("mount: ", source)
		matched, _ := regexp.MatchString("slaves.*frameworks.*executors", source)
		if matched {
			dir = source
		}
	}
	url := fmt.Sprintf("http://%s.%s:5051/files/read.json?path=%s/stdout&offset=%d&length=32000",
		firstMtsd.Config.Hostname, firstMtsd.Config.Domainname, dir, offset)
	bodyData, _ := FetchLog(url)

	var logData LogData
	err = json.Unmarshal(bodyData, &logData)
	if err != nil {
		return nil, err
	}
	return []byte(logData.Data), nil
}
