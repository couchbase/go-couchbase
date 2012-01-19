package couchbase

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
)

type PoolsResponse struct {
	ComponentsVersion     map[string]string
	ImplementationVersion string
	IsAdmin               bool `json:"isAdminCreds"`
	UUID                  string
	Pools                 []struct {
		Name         string
		StreamingURI string
		URI          string
	}
}

type Node struct {
	ClusterCompatibility int
	ClusterMembership    string
	CouchAPIBase         string
	Hostname             string
	InterestingStats     map[string]uint64
	MCDMemoryAllocated   uint64
	MCDMemoryReserved    uint64
	MemoryFree           uint64
	MemoryTotal          uint64
	OS                   string
	Ports                map[string]int
	Status               string
	Uptime               int `json:"uptime,string"`
	Version              string
}

type Pool struct {
	Buckets map[string]string
	Nodes   []Node

	client Client
}

type Client struct {
	BaseURL *url.URL
	Info    PoolsResponse
}

func (c *Client) parseURLResponse(path string, out interface{}) error {
	u := *c.BaseURL
	u.Path = path

	res, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer res.Body.Close()

	d := json.NewDecoder(res.Body)
	if err = d.Decode(&out); err != nil {
		return err
	}
	return nil
}

func Connect(baseU string) (c Client, err error) {
	c.BaseURL, err = url.Parse(baseU)
	if err != nil {
		return
	}
	err = c.parseURLResponse("/pools", &c.Info)
	return
}

func (c *Client) GetPool(name string) (p Pool, err error) {
	var poolURI string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolURI = p.URI
		}
	}
	if poolURI == "" {
		return p, errors.New("No pool named " + name)
	}
	err = c.parseURLResponse(poolURI, &p)
	p.client = *c
	return
}

func (p *Pool) GetBuckets() error {
	return nil
}
