package couchbase

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/dustin/gomemcached/client"
)

type poolsResponse struct {
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

// A computer in a cluster running the couchbase software.
type Node struct {
	ClusterCompatibility int
	ClusterMembership    string
	CouchAPIBase         string
	Hostname             string
	InterestingStats     map[string]float64
	MCDMemoryAllocated   float64
	MCDMemoryReserved    float64
	MemoryFree           float64
	MemoryTotal          float64
	OS                   string
	Ports                map[string]int
	Status               string
	Uptime               int `json:"uptime,string"`
	Version              string
}

// A pool of nodes and buckets.
type Pool struct {
	BucketMap map[string]Bucket
	Nodes     []Node

	BucketURL map[string]string `json:"buckets"`

	client Client
}

// An individual bucket.  Herein lives the most useful stuff.
type Bucket struct {
	AuthType            string
	Capabilities        []string `json:"bucketCapabilities"`
	CapabilitiesVersion string   `json:"bucketCapabilitiesVer"`
	Type                string   `json:"bucketType"`
	Name                string
	NodeLocator         string
	Nodes               []Node
	Quota               map[string]float64
	Replicas            int    `json:"replicaNumber"`
	Password            string `json:"saslPassword"`
	URI                 string
	VBucketServerMap    struct {
		HashAlgorithm string
		NumReplicas   int
		ServerList    []string
		VBucketMap    [][]int
	}

	pool        *Pool
	connections []*memcached.Client
}

// The couchbase client gives access to all the things.
type Client struct {
	BaseURL  *url.URL
	Info     poolsResponse
	Statuses [256]uint64
}

func (c *Client) parseURLResponse(path string, out interface{}) error {
	u := *c.BaseURL
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

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

// Connect to a couchbase cluster.
func Connect(baseU string) (c Client, err error) {
	c.BaseURL, err = url.Parse(baseU)
	if err != nil {
		return
	}
	err = c.parseURLResponse("/pools", &c.Info)
	return
}

func (b *Bucket) refresh() (err error) {
	pool := b.pool
	err = pool.client.parseURLResponse(b.URI, b)
	if err != nil {
		return err
	}
	b.pool = pool
	b.connections = make([]*memcached.Client, len(b.VBucketServerMap.ServerList))
	return nil
}

func (p *Pool) refresh() (err error) {
	p.BucketMap = make(map[string]Bucket)

	buckets := []Bucket{}
	err = p.client.parseURLResponse(p.BucketURL["uri"], &buckets)
	if err != nil {
		return err
	}
	for _, b := range buckets {
		b.pool = p
		b.connections = make([]*memcached.Client, len(b.VBucketServerMap.ServerList))
		p.BucketMap[b.Name] = b
	}
	return nil
}

// Get a pool from within the couchbase cluster (usually "default").
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

	p.refresh()
	return
}

// Get a bucket from within this pool.
func (p *Pool) GetBucket(name string) (b Bucket, err error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return Bucket{}, errors.New("No bucket named " + name)
	}
	return rv, nil
}
