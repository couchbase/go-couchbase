package couchbase

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"

	"github.com/dustin/gomemcached/client"
)

// The HTTP Client To Use
var HttpClient = http.DefaultClient

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
	connections []*connectionPool
	commonSufix string
}

// Get the (sorted) list of memcached node addresses (hostname:port).
func (b Bucket) NodeAddresses() []string {
	rv := make([]string, len(b.VBucketServerMap.ServerList))
	copy(rv, b.VBucketServerMap.ServerList)
	sort.Strings(rv)
	return rv
}

// Get the longest common suffix of all host:port strings in the node list.
func (b Bucket) CommonAddressSuffix() string {
	input := []string{}
	for _, n := range b.Nodes {
		input = append(input, n.Hostname)
	}
	return FindCommonSuffix(input)
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

	res, err := HttpClient.Get(u.String())
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
	b.connections = make([]*connectionPool, len(b.VBucketServerMap.ServerList))
	for i := range b.connections {
		b.connections[i] = &connectionPool{
			host:        b.VBucketServerMap.ServerList[i],
			name:        b.Name,
			connections: []*memcached.Client{},
		}
	}
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
		b.connections = make([]*connectionPool, len(b.VBucketServerMap.ServerList))

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

// Mark this bucket as no longer needed, closing connections it may have open.
func (b *Bucket) Close() {
	if b.connections != nil {
		for _, c := range b.connections {
			if c != nil {
				c.Close()
			}
		}
		b.connections = nil
	}
}

func bucket_finalizer(b *Bucket) {
	if b.connections != nil {
		log.Printf("Warning: Finalizing a bucket with active connections.")
	}
}

// Get a bucket from within this pool.
func (p *Pool) GetBucket(name string) (b *Bucket, err error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return nil, errors.New("No bucket named " + name)
	}
	runtime.SetFinalizer(&rv, bucket_finalizer)
	rv.refresh()
	return &rv, nil
}

// Get the pool to which this bucket belongs.
func (b *Bucket) GetPool() *Pool {
	return b.pool
}

// Get the client from which we got this pool.
func (p *Pool) GetClient() *Client {
	return &p.client
}

// Convenience function for getting a named bucket from a URL
func GetBucket(endpoint, poolname, bucketname string) (*Bucket, error) {
	var err error
	client, err := Connect(endpoint)
	if err != nil {
		return nil, err
	}

	pool, err := client.GetPool(poolname)
	if err != nil {
		return nil, err
	}

	return pool.GetBucket(bucketname)
}
