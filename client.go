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
*/
package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

func (b *Bucket) ensureConnection(which int) error {
	if b.connections == nil {
		b.connections = []*memcached.Client{}
	}
	if b.connections[which] == nil {
		var err error
		b.connections[which], err = memcached.Connect("tcp",
			b.VBucketServerMap.ServerList[which])
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Bucket) do(k string, f func(mc *memcached.Client, vb uint16) error) error {
	vb := b.VBHash(k)
	for {
		masterId := b.VBucketServerMap.VBucketMap[vb][0]
		if err := b.ensureConnection(masterId); err != nil {
			return err
		}
		err := f(b.connections[masterId], uint16(vb))
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
	err := b.ensureConnection(offset)
	if err != nil {
		ch <- gathered_stats{sn, results}
	} else {
		st, err := b.connections[offset].StatsMap(which)
		if err == nil {
			ch <- gathered_stats{sn, st}
		} else {
			ch <- gathered_stats{sn, results}
		}
	}
}

func (b *Bucket) GetStats(which string) map[string]map[string]string {
	// Go grab all the things at once.
	todo := len(b.VBucketServerMap.ServerList)
	ch := make(chan gathered_stats, todo)

	for offset, _ := range b.VBucketServerMap.ServerList {
		go getStatsParallel(b, offset, which, ch)
	}

	// Gather the results
	rv := map[string]map[string]string{}
	for i := 0; i < len(b.VBucketServerMap.ServerList); i++ {
		g := <-ch
		if len(g.vals) > 0 {
			rv[g.sn] = g.vals
		}
	}

	return rv
}

// Set a value in this bucket.
// The value will be serialized into a JSON document.
func (b *Bucket) Set(k string, v interface{}) error {
	return b.do(k, func(mc *memcached.Client, vb uint16) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		res, err := mc.Set(vb, k, 0, 0, data)
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
func (b *Bucket) Get(k string, rv interface{}) error {
	return b.do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k)
		if err != nil {
			return err
		}
		if res.Status != gomemcached.SUCCESS {
			return res
		}
		return json.Unmarshal(res.Body, rv)
	})
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string) error {
	return b.do(k, func(mc *memcached.Client, vb uint16) error {
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

// Execute a view
func (b *Bucket) View(ddoc, name string, params map[string]interface{}) (vres ViewResult, err error) {
	// Pick a random node to service our request.
	node := b.Nodes[rand.Intn(len(b.Nodes))]
	u, err := url.Parse(node.CouchAPIBase)
	if err != nil {
		return ViewResult{}, err
	}

	values := url.Values{}
	for k, v := range params {
		values[k] = []string{fmt.Sprintf("%v", v)}
	}

	u.Path = fmt.Sprintf("/%s/_design/%s/_view/%s", b.Name, ddoc, name)
	u.RawQuery = values.Encode()

	res, err := http.Get(u.String())
	if err != nil {
		return ViewResult{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return ViewResult{}, errors.New(res.Status)
	}

	d := json.NewDecoder(res.Body)
	if err = d.Decode(&vres); err != nil {
		return ViewResult{}, err
	}
	return vres, nil

}
