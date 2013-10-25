package couchbase

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"unsafe"
)

// The HTTP Client To Use
var HttpClient = http.DefaultClient

// Size of the connection pools (per host).
var PoolSize = 4

// Number of overflow connections allowed in a pool.
var PoolOverflow = PoolSize

// Auth callback gets the auth username and password for the given
// bucket.
type AuthHandler interface {
	GetCredentials() (string, string)
}

type RestPool struct {
	Name         string `json:"name"`
	StreamingURI string `json:"streamingUri"`
	URI          string `json:"uri"`
}

type Pools struct {
	ComponentsVersion     map[string]string `json:"componentsVersion,omitempty"`
	ImplementationVersion string            `json:"implementationVersion"`
	IsAdmin               bool              `json:"isAdminCreds"`
	UUID                  string            `json:"uuid"`
	Pools                 []RestPool        `json:"pools"`
}

// A computer in a cluster running the couchbase software.
type Node struct {
	ClusterCompatibility int                `json:"clusterCompatibility"`
	ClusterMembership    string             `json:"clusterMembership"`
	CouchAPIBase         string             `json:"couchApiBase"`
	Hostname             string             `json:"hostname"`
	InterestingStats     map[string]float64 `json:"interestingStats,omitempty"`
	MCDMemoryAllocated   float64            `json:"mcdMemoryAllocated"`
	MCDMemoryReserved    float64            `json:"mcdMemoryReserved"`
	MemoryFree           float64            `json:"memoryFree"`
	MemoryTotal          float64            `json:"memoryTotal"`
	OS                   string             `json:"os"`
	Ports                map[string]int     `json:"ports"`
	Status               string             `json:"status"`
	Uptime               int                `json:"uptime,string"`
	Version              string             `json:"version"`
	ThisNode             bool               `json:"thisNode,omitempty"`
}

// A pool of nodes and buckets.
type Pool struct {
	BucketMap map[string]Bucket
	Nodes     []Node

	BucketURL map[string]string `json:"buckets"`

	client Client
}

type VBucketServerMap struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap"`
}

// An individual bucket.  Herein lives the most useful stuff.
type Bucket struct {
	AuthType            string             `json:"authType"`
	Capabilities        []string           `json:"bucketCapabilities"`
	CapabilitiesVersion string             `json:"bucketCapabilitiesVer"`
	Type                string             `json:"bucketType"`
	Name                string             `json:"name"`
	NodeLocator         string             `json:"nodeLocator"`
	Quota               map[string]float64 `json:"quota,omitempty"`
	Replicas            int                `json:"replicaNumber"`
	Password            string             `json:"saslPassword"`
	URI                 string             `json:"uri"`
	StreamingURI        string             `json:"streamingUri"`
	LocalRandomKeyURI   string             `json:"localRandomKeyUri,omitempty"`
	UUID                string             `json:"uuid"`
	DDocs               struct {
		URI string `json:"uri"`
	} `json:"ddocs,omitempty"`
	BasicStats  map[string]interface{} `json:"basicStats,omitempty"`
	Controllers map[string]interface{} `json:"controllers,omitempty"`

	// These are used for JSON IO, but isn't used for processing
	// since it needs to be swapped out safely.
	VBSMJson  VBucketServerMap `json:"vBucketServerMap"`
	NodesJson []Node           `json:"nodes"`

	pool             *Pool
	connPools        unsafe.Pointer // *[]*connectionPool
	vBucketServerMap unsafe.Pointer // *VBucketServerMap
	nodeList         unsafe.Pointer // *[]Node
	commonSufix      string
}

// Get the current vbucket server map
func (b Bucket) VBServerMap() *VBucketServerMap {
	return (*VBucketServerMap)(atomic.LoadPointer(&b.vBucketServerMap))
}

func (b Bucket) Nodes() []Node {
	return *(*[]Node)(atomic.LoadPointer(&b.nodeList))
}

func (b Bucket) getConnPools() []*connectionPool {
	return *(*[]*connectionPool)(atomic.LoadPointer(&b.connPools))
}

func (b *Bucket) replaceConnPools(with []*connectionPool) {
	for {
		old := atomic.LoadPointer(&b.connPools)
		if atomic.CompareAndSwapPointer(&b.connPools, old, unsafe.Pointer(&with)) {
			if old != nil {
				for _, pool := range *(*[]*connectionPool)(old) {
					pool.Close()
				}
			}
			return
		}
	}
}

func (b Bucket) getConnPool(i int) *connectionPool {
	p := b.getConnPools()
	if len(p) > i {
		return p[i]
	}
	return nil
}

func (b Bucket) authHandler() (ah AuthHandler) {
	if b.pool != nil {
		ah = b.pool.client.ah
	}
	if ah == nil {
		ah = &basicAuth{b.Name, ""}
	}
	return
}

// Get the (sorted) list of memcached node addresses (hostname:port).
func (b Bucket) NodeAddresses() []string {
	vsm := b.VBServerMap()
	rv := make([]string, len(vsm.ServerList))
	copy(rv, vsm.ServerList)
	sort.Strings(rv)
	return rv
}

// Get the longest common suffix of all host:port strings in the node list.
func (b Bucket) CommonAddressSuffix() string {
	input := []string{}
	for _, n := range b.Nodes() {
		input = append(input, n.Hostname)
	}
	return FindCommonSuffix(input)
}

// The couchbase client gives access to all the things.
type Client struct {
	BaseURL  *url.URL
	ah       AuthHandler
	Info     Pools
	Statuses [256]uint64
}

func maybeAddAuth(req *http.Request, ah AuthHandler) {
	if ah != nil {
		user, pass := ah.GetCredentials()
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}
}

func (c *Client) parseURLResponse(path string, out interface{}) error {
	u := *c.BaseURL
	u.User = nil
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	maybeAddAuth(req, c.ah)

	res, err := HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("HTTP error %v getting %q: %s",
			res.Status, u.String(), bod)
	}

	d := json.NewDecoder(res.Body)
	if err = d.Decode(&out); err != nil {
		return err
	}
	return nil
}

type basicAuth struct {
	u, p string
}

func (b basicAuth) GetCredentials() (string, string) {
	return b.u, b.p
}

func basicAuthFromURL(us string) (ah AuthHandler) {
	u, err := ParseURL(us)
	if err != nil {
		return
	}
	if user := u.User; user != nil {
		pw, _ := user.Password()
		ah = basicAuth{user.Username(), pw}
	}
	return
}

// ConnectWithAuth connects to a couchbase cluster with the given
// authentication handler.
func ConnectWithAuth(baseU string, ah AuthHandler) (c Client, err error) {
	c.BaseURL, err = ParseURL(baseU)
	if err != nil {
		return
	}
	c.ah = ah

	return c, c.parseURLResponse("/pools", &c.Info)
}

// Connect to a couchbase cluster.  An authentication handler will be
// created from the userinfo in the URL if provided.
func Connect(baseU string) (Client, error) {
	return ConnectWithAuth(baseU, basicAuthFromURL(baseU))
}

func (b *Bucket) refresh() error {
	pool := b.pool
	tmpb := &Bucket{}
	err := pool.client.parseURLResponse(b.URI, tmpb)
	if err != nil {
		return err
	}
	newcps := make([]*connectionPool, len(tmpb.VBSMJson.ServerList))
	for i := range newcps {
		newcps[i] = newConnectionPool(
			tmpb.VBSMJson.ServerList[i],
			b.authHandler(), PoolSize, PoolOverflow)
	}
	b.replaceConnPools(newcps)
	atomic.StorePointer(&b.vBucketServerMap, unsafe.Pointer(&tmpb.VBSMJson))
	atomic.StorePointer(&b.nodeList, unsafe.Pointer(&tmpb.NodesJson))
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
		b.nodeList = unsafe.Pointer(&b.NodesJson)
		b.replaceConnPools(make([]*connectionPool, len(b.VBSMJson.ServerList)))

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

	err = p.refresh()
	return
}

// Mark this bucket as no longer needed, closing connections it may have open.
func (b *Bucket) Close() {
	if b.connPools != nil {
		for _, c := range b.getConnPools() {
			if c != nil {
				c.Close()
			}
		}
		b.connPools = nil
	}
}

func bucket_finalizer(b *Bucket) {
	if b.connPools != nil {
		log.Printf("Warning: Finalizing a bucket with active connections.")
	}
}

// Get a bucket from within this pool.
func (p *Pool) GetBucket(name string) (*Bucket, error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return nil, errors.New("No bucket named " + name)
	}
	runtime.SetFinalizer(&rv, bucket_finalizer)
	err := rv.refresh()
	if err != nil {
		return nil, err
	}
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
