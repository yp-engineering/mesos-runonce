package mesosutil

import (
	"net/http"
	"time"

	log "github.com/golang/glog"
)

var defaultClient = NewClient()

func NewClient() *Client {
	c := &Client{}

	c.Transport = NewTransport()
	c.Client.Transport = c.Transport

	return c
}

type Client struct {
	http.Client
	*Transport
}

func NewTransport() *Transport {
	t := *http.DefaultTransport.(*http.Transport)

	return &Transport{
		Transport: t,
	}
}

type Transport struct {
	http.Transport
	Debug bool
}

func (t *Transport) RoundTrip(r *http.Request) (resp *http.Response, err error) {
	now := time.Now()

	defer func() {
		if true { // TODO get debug config value
			if err != nil {
				log.Errorf("[client] transport error %#v\n", err)
				return
			}
			// Time GET / HTTP1/1 200 (2ms)
			log.V(1).Infof("%s %v %s %d (%v)\n",
				r.Method, r.URL, r.Proto, resp.StatusCode, time.Since(now))
		}
	}()

	return t.Transport.RoundTrip(r)
}
