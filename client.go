/*
A smart client for go.

Usage:

 client, err := couchbase.Connect("http://myserver:8091/")
 handleError(err)
 pool, err := client.GetPool("default")
 handleError(err)
 bucket, err := pool.getBucket("MyAwesomeBucket")
 handleError(err)
 ...

or a shortcut for the bucket directly

 bucket, err := couchbase.GetBucket("http://myserver:8091/", "default", "default")
*/
package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type connectionPool struct {
	host        string
	name        string
	connections []*memcached.Client
	mutex       sync.Mutex
}

func (cp *connectionPool) Close() error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	for _, c := range cp.connections {
		c.Close()
	}
	cp.connections = []*memcached.Client{}
	return nil
}

func (cp *connectionPool) Get() (*memcached.Client, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if len(cp.connections) == 0 {
		conn, err := memcached.Connect("tcp", cp.host)
		if err != nil {
			return nil, err
		}
		if cp.name != "default" {
			conn.Auth(cp.name, "")
		}

		cp.connections = append(cp.connections, conn)
	}

	rv := cp.connections[0]
	cp.connections = cp.connections[1:]

	return rv, nil
}

func (cp *connectionPool) Return(c *memcached.Client) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if c != nil {
		if c.IsHealthy() {
			cp.connections = append(cp.connections, c)
		} else {
			c.Close()
		}
	}
}

// Execute a function on a memcached connection to the node owning key "k"
//
// Note that this automatically handles transient errors by replaying
// your function on a "not-my-vbucket" error, so don't assume
// your command will only be executed only once.
func (b *Bucket) Do(k string, f func(mc *memcached.Client, vb uint16) error) error {
	vb := b.VBHash(k)
	for {
		masterId := b.VBucketServerMap.VBucketMap[vb][0]
		conn, err := b.connections[masterId].Get()
		defer b.connections[masterId].Return(conn)
		if err != nil {
			return err
		}

		err = f(conn, uint16(vb))
		switch err.(type) {
		default:
			return err
		case gomemcached.MCResponse:
			st := err.(gomemcached.MCResponse).Status
			atomic.AddUint64(&b.pool.client.Statuses[st], 1)
			if st == gomemcached.NOT_MY_VBUCKET {
				b.refresh()
			} else {
				return err
			}
		}
	}
	panic("Unreachable.")
}

type gathered_stats struct {
	sn   string
	vals map[string]string
}

func getStatsParallel(b *Bucket, offset int, which string, ch chan<- gathered_stats) {
	sn := b.VBucketServerMap.ServerList[offset]

	results := map[string]string{}
	conn, err := b.connections[offset].Get()
	defer b.connections[offset].Return(conn)
	if err != nil {
		ch <- gathered_stats{sn, results}
	} else {
		st, err := conn.StatsMap(which)
		if err == nil {
			ch <- gathered_stats{sn, st}
		} else {
			ch <- gathered_stats{sn, results}
		}
	}
}

// Get a set of stats from all servers.
//
// Returns a map of server ID -> map of stat key to map value.
func (b *Bucket) GetStats(which string) map[string]map[string]string {
	rv := map[string]map[string]string{}

	if b.VBucketServerMap.ServerList == nil {
		return rv
	}
	// Go grab all the things at once.
	todo := len(b.VBucketServerMap.ServerList)
	ch := make(chan gathered_stats, todo)

	for offset, _ := range b.VBucketServerMap.ServerList {
		go getStatsParallel(b, offset, which, ch)
	}

	// Gather the results
	for i := 0; i < len(b.VBucketServerMap.ServerList); i++ {
		g := <-ch
		if len(g.vals) > 0 {
			rv[g.sn] = g.vals
		}
	}

	return rv
}

func (b *Bucket) doBulkGet(vb uint16, keys []string,
	ch chan<- map[string]*gomemcached.MCResponse) {

	masterId := b.VBucketServerMap.VBucketMap[vb][0]
	conn, err := b.connections[masterId].Get()
	if err != nil {
		ch <- map[string]*gomemcached.MCResponse{}
	}
	defer b.connections[masterId].Return(conn)

	m, err := conn.GetBulk(vb, keys)
	switch err.(type) {
	default:
		ch <- m
	case *gomemcached.MCResponse:
		fmt.Printf("Got a memcached error")
		st := err.(gomemcached.MCResponse).Status
		atomic.AddUint64(&b.pool.client.Statuses[st], 1)
		if st == gomemcached.NOT_MY_VBUCKET {
			b.refresh()
		}
		ch <- map[string]*gomemcached.MCResponse{}
	}
}

func (b *Bucket) processBulkGet(kdm map[uint16][]string,
	ch chan map[string]*gomemcached.MCResponse) {

	wch := make(chan uint16)

	worker := func() {
		for k := range wch {
			b.doBulkGet(k, kdm[k], ch)
		}
	}

	for i := 0; i < 4; i++ {
		go worker()
	}

	for k := range kdm {
		wch <- k
	}
	close(wch)

}
func (b *Bucket) GetBulk(keys []string) map[string]*gomemcached.MCResponse {
	// Organize by vbucket
	kdm := map[uint16][]string{}
	for _, k := range keys {
		vb := uint16(b.VBHash(k))
		a, ok := kdm[vb]
		if !ok {
			a = []string{}
		}
		kdm[vb] = append(a, k)
	}

	ch := make(chan map[string]*gomemcached.MCResponse)
	defer close(ch)

	go b.processBulkGet(kdm, ch)

	rv := map[string]*gomemcached.MCResponse{}
	for _ = range kdm {
		m := <-ch
		for k, v := range m {
			rv[k] = v
		}
	}

	return rv
}

// Set a value in this bucket.
// The value will be serialized into a JSON document.
func (b *Bucket) Set(k string, exp int, v interface{}) error {
	return b.Do(k, func(mc *memcached.Client, vb uint16) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		res, err := mc.Set(vb, k, 0, exp, data)
		if err != nil {
			return err
		}
		if res.Status != gomemcached.SUCCESS {
			return res
		}
		return nil
	})
}

// Get a value from this bucket.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Gets(k string, rv interface{}, cas *uint64) error {
	return b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k)
		if err != nil {
			return err
		}
		if res.Status != gomemcached.SUCCESS {
			return res
		}
		if cas != nil {
			*cas = res.Cas
		}
		return json.Unmarshal(res.Body, rv)
	})
}

// Get a value from this bucket.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Get(k string, rv interface{}) error {
	return b.Gets(k, rv, nil)
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string) error {
	return b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Del(vb, k)
		if err != nil {
			return err
		}
		if res.Status != gomemcached.SUCCESS {
			return res
		}
		return nil
	})
}

// Increment a key
func (b *Bucket) Incr(k string, amt, def uint64, exp int) (uint64, error) {
	var rv uint64
	err := b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Incr(vb, k, amt, def, exp)
		if err != nil {
			return err
		}
		rv = res
		return nil
	})
	return rv, err
}

type ViewRow struct {
	ID    string
	Key   interface{}
	Value interface{}
	Doc   *interface{}
}

type ViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      []ViewRow
	Errors    []struct {
		From   string
		Reason string
	}
}

// Perform a view request that can map row values to a custom type.
//
// See the source to View for an example usage.
func (b *Bucket) ViewCustom(ddoc, name string, params map[string]interface{},
	vres interface{}) error {

	// Pick a random node to service our request.
	node := b.Nodes[rand.Intn(len(b.Nodes))]
	u, err := url.Parse(node.CouchAPIBase)
	if err != nil {
		return err
	}

	values := url.Values{}
	for k, v := range params {
		switch t := v.(type) {
		case string:
			values[k] = []string{fmt.Sprintf(`"%s"`, url.QueryEscape(t))}
		case int:
			values[k] = []string{fmt.Sprintf(`%d`, t)}
		case bool:
			values[k] = []string{fmt.Sprintf(`%v`, t)}
		default:
			b, err := json.Marshal(v)
			if err != nil {
				panic(fmt.Sprintf("unsupported value-type %T in Query, json encoder said %v", t, err))
			}
			values[k] = []string{fmt.Sprintf(`%v`, string(b))}
		}
	}

	u.Path = fmt.Sprintf("/%s/_design/%s/_view/%s", b.Name, ddoc, name)
	u.RawQuery = values.Encode()

	res, err := http.Get(u.String())
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}

	d := json.NewDecoder(res.Body)
	if err := d.Decode(vres); err != nil {
		return err
	}
	return nil
}

// Execute a view
func (b *Bucket) View(ddoc, name string, params map[string]interface{}) (ViewResult, error) {
	vres := ViewResult{}
	return vres, b.ViewCustom(ddoc, name, params, &vres)
}
