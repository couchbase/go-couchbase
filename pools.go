package couchbase

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/gomemcached/client" // package name is 'memcached'
)

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 256
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

// PoolSize is the size of each connection pool (per host).
var PoolSize = 4

// PoolOverflow is the number of overflow connections allowed in a
// pool.
var PoolOverflow = PoolSize

// AuthHandler is a callback that gets the auth username and password
// for the given bucket.
type AuthHandler interface {
	GetCredentials() (string, string)
}

// RestPool represents a single pool returned from the pools REST API.
type RestPool struct {
	Name         string `json:"name"`
	StreamingURI string `json:"streamingUri"`
	URI          string `json:"uri"`
}

// Pools represents the collection of pools as returned from the REST API.
type Pools struct {
	ComponentsVersion     map[string]string `json:"componentsVersion,omitempty"`
	ImplementationVersion string            `json:"implementationVersion"`
	IsAdmin               bool              `json:"isAdminCreds"`
	UUID                  string            `json:"uuid"`
	Pools                 []RestPool        `json:"pools"`
}

// A Node is a computer in a cluster running the couchbase software.
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

// A Pool of nodes and buckets.
type Pool struct {
	BucketMap map[string]Bucket
	Nodes     []Node

	BucketURL map[string]string `json:"buckets"`

	client Client
}

// VBucketServerMap is the a mapping of vbuckets to nodes.
type VBucketServerMap struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap"`
}

// Bucket is the primary entry point for most data operations.
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
	NodesJSON []Node           `json:"nodes"`

	pool             *Pool
	connPools        unsafe.Pointer // *[]*connectionPool
	vBucketServerMap unsafe.Pointer // *VBucketServerMap
	nodeList         unsafe.Pointer // *[]Node
	commonSufix      string
	ah               AuthHandler // auth handler
}

// PoolServices is all the bucket-independent services in a pool
type PoolServices struct {
	Rev      int            `json:"rev"`
	NodesExt []NodeServices `json:"nodesExt"`
}

// NodeServices is all the bucket-independent services running on
// a node (given by Hostname)
type NodeServices struct {
	Services map[string]int `json:"services,omitempty"`
	Hostname string         `json:"hostname"`
	ThisNode bool           `json:"thisNode"`
}

type BucketAuth struct {
	name    string
	saslPwd string
}

func newBucketAuth(name string, pass string) *BucketAuth {
	return &BucketAuth{name: name, saslPwd: pass}
}

func (ba *BucketAuth) GetCredentials() (string, string) {
	return ba.name, ba.saslPwd
}

// VBServerMap returns the current VBucketServerMap.
func (b *Bucket) VBServerMap() *VBucketServerMap {
	return (*VBucketServerMap)(atomic.LoadPointer(&(b.vBucketServerMap)))
}

func (b *Bucket) GetVBmap(addrs []string) (map[string][]uint16, error) {
	vbmap := b.VBServerMap()
	servers := vbmap.ServerList
	if addrs == nil {
		addrs = vbmap.ServerList
	}

	m := make(map[string][]uint16)
	for _, addr := range addrs {
		m[addr] = make([]uint16, 0)
	}
	for vbno, idxs := range vbmap.VBucketMap {
		addr := servers[idxs[0]]
		if _, ok := m[addr]; ok {
			m[addr] = append(m[addr], uint16(vbno))
		}
	}
	return m, nil
}

// Nodes returns teh current list of nodes servicing this bucket.
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
					if pool != nil {
						pool.Close()
					}
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

// Given a vbucket number, returns a memcached connection to it.
// The connection must be returned to its pool after use.
func (b Bucket) getConnectionToVBucket(vb uint32) (*memcached.Client, *connectionPool, error) {
	for {
		vbm := b.VBServerMap()
		if len(vbm.VBucketMap) < int(vb) {
			return nil, nil, fmt.Errorf("go-couchbase: vbmap smaller than vbucket list: %v vs. %v",
				vb, vbm.VBucketMap)
		}
		masterId := vbm.VBucketMap[vb][0]
		pool := b.getConnPool(masterId)
		conn, err := pool.Get()
		if err != errClosedPool {
			return conn, pool, err
		}
		// If conn pool was closed, because another goroutine refreshed the vbucket map, retry...
	}
}

func (b Bucket) getMasterNode(i int) string {
	p := b.getConnPools()
	if len(p) > i {
		return p[i].host
	}
	return ""
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

// NodeAddresses gets the (sorted) list of memcached node addresses
// (hostname:port).
func (b Bucket) NodeAddresses() []string {
	vsm := b.VBServerMap()
	rv := make([]string, len(vsm.ServerList))
	copy(rv, vsm.ServerList)
	sort.Strings(rv)
	return rv
}

// CommonAddressSuffix finds the longest common suffix of all
// host:port strings in the node list.
func (b Bucket) CommonAddressSuffix() string {
	input := []string{}
	for _, n := range b.Nodes() {
		input = append(input, n.Hostname)
	}
	return FindCommonSuffix(input)
}

// A Client is the starting point for all services across all buckets
// in a Couchbase cluster.
type Client struct {
	BaseURL *url.URL
	ah      AuthHandler
	Info    Pools
}

func maybeAddAuth(req *http.Request, ah AuthHandler) {
	if ah != nil {
		user, pass := ah.GetCredentials()
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}
}

func queryRestAPI(
	baseURL *url.URL,
	path string,
	authHandler AuthHandler,
	out interface{}) error {
	u := *baseURL
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
	maybeAddAuth(req, authHandler)

	res, err := HTTPClient.Do(req)
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

func (c *Client) parseURLResponse(path string, out interface{}) error {
	return queryRestAPI(c.BaseURL, path, c.ah, out)
}

func (b *Bucket) parseURLResponse(path string, out interface{}) error {
	nodes := b.Nodes()
	if len(nodes) == 0 {
		return errors.New("no couch rest URLs")
	}

	// Pick a random node to start querying.
	startNode := rand.Intn(len(nodes))
	maxRetries := len(nodes)
	for i := 0; i < maxRetries; i++ {
		node := nodes[(startNode+i)%len(nodes)] // Wrap around the nodes list.
		// Skip non-healthy nodes.
		if node.Status != "healthy" {
			continue
		}

		url := &url.URL{
			Host:   node.Hostname,
			Scheme: "http",
		}

		err := queryRestAPI(url, path, b.pool.client.ah, out)
		if err == nil {
			return err
		}
	}
	return errors.New("all nodes failed to respond")
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

type BucketInfo struct {
	Name     string // name of bucket
	Password string // SASL password of bucket
}

//Get SASL buckets
func GetBucketList(baseU string) (bInfo []BucketInfo, err error) {

	c := &Client{}
	c.BaseURL, err = ParseURL(baseU)
	if err != nil {
		return
	}
	c.ah = basicAuthFromURL(baseU)

	var buckets []Bucket
	err = c.parseURLResponse("/pools/default/buckets", &buckets)
	if err != nil {
		return
	}
	bInfo = make([]BucketInfo, 0)
	for _, bucket := range buckets {
		bucketInfo := BucketInfo{Name: bucket.Name, Password: bucket.Password}
		bInfo = append(bInfo, bucketInfo)
	}
	return bInfo, err
}

func (b *Bucket) Refresh() error {
	pool := b.pool
	tmpb := &Bucket{}
	err := pool.client.parseURLResponse(b.URI, tmpb)
	if err != nil {
		return err
	}
	newcps := make([]*connectionPool, len(tmpb.VBSMJson.ServerList))
	for i := range newcps {
		if b.ah != nil {
			newcps[i] = newConnectionPool(
				tmpb.VBSMJson.ServerList[i],
				b.ah, PoolSize, PoolOverflow)

		} else {
			newcps[i] = newConnectionPool(
				tmpb.VBSMJson.ServerList[i],
				b.authHandler(), PoolSize, PoolOverflow)
		}
	}
	b.replaceConnPools(newcps)
	tmpb.ah = b.ah
	atomic.StorePointer(&b.vBucketServerMap, unsafe.Pointer(&tmpb.VBSMJson))
	atomic.StorePointer(&b.nodeList, unsafe.Pointer(&tmpb.NodesJSON))
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
		b.nodeList = unsafe.Pointer(&b.NodesJSON)
		b.replaceConnPools(make([]*connectionPool, len(b.VBSMJson.ServerList)))

		p.BucketMap[b.Name] = b
	}
	return nil
}

// GetPool gets a pool from within the couchbase cluster (usually
// "default").
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

// GetPoolServices returns all the bucket-independent services in a pool.
// (See "Exposing services outside of bucket context" in http://goo.gl/uuXRkV)
func (c *Client) GetPoolServices(name string) (ps PoolServices, err error) {
	var poolName string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolName = p.Name
		}
	}
	if poolName == "" {
		return ps, errors.New("No pool named " + name)
	}

	poolURI := "/pools/" + poolName + "/nodeServices"
	err = c.parseURLResponse(poolURI, &ps)

	return
}

// Close marks this bucket as no longer needed, closing connections it
// may have open.
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

func bucketFinalizer(b *Bucket) {
	if b.connPools != nil {
		log.Printf("Warning: Finalizing a bucket with active connections.")
	}
}

// GetBucket gets a bucket from within this pool.
func (p *Pool) GetBucket(name string) (*Bucket, error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return nil, errors.New("No bucket named " + name)
	}
	runtime.SetFinalizer(&rv, bucketFinalizer)
	err := rv.Refresh()
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

// GetBucket gets a bucket from within this pool.
func (p *Pool) GetBucketWithAuth(name string, password string) (*Bucket, error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return nil, errors.New("No bucket named " + name)
	}
	runtime.SetFinalizer(&rv, bucketFinalizer)
	rv.ah = newBucketAuth(name, password)
	err := rv.Refresh()
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

// GetPool gets the pool to which this bucket belongs.
func (b *Bucket) GetPool() *Pool {
	return b.pool
}

// GetClient gets the client from which we got this pool.
func (p *Pool) GetClient() *Client {
	return &p.client
}

// GetBucket is a convenience function for getting a named bucket from
// a URL
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
