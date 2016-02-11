package mesosutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func MesosLogs(host, taskId, iotype string, offset, length int) ([]byte, error) {
	stateUrl := "http://" + host + "/state.json"

	resp, err := defaultClient.Get(stateUrl)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	var ms MesosState
	err = json.NewDecoder(resp.Body).Decode(&ms)
	if err != nil {
		return nil, err
	}

	dir := ms.Directory(taskId)

	if dir == "" {
		return nil, ProxyError{"directory not found", http.StatusInternalServerError}
	}

	logUrl := fmt.Sprintf("http://%s/files/read.json?path=%s/%s&offset=%d&length=%d",
		host, dir, iotype, offset, length)

	return FetchLog(logUrl)
}

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
