/*
A smart client for go.

Usage:

 client, err := couchbase.Connect("http://myserver:8091/")
 handleError(err)
 pool, err := client.GetPool("default")
 handleError(err)
 bucket, err := pool.GetBucket("MyAwesomeBucket")
 handleError(err)
 ...

or a shortcut for the bucket directly

 bucket, err := couchbase.GetBucket("http://myserver:8091/", "default", "default")
*/
package couchbase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync/atomic"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

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
}

type gathered_stats struct {
	sn   string
	vals map[string]string
}

func getStatsParallel(b *Bucket, offset int, which string,
	ch chan<- gathered_stats) {
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

	for offset := range b.VBucketServerMap.ServerList {
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
		return
	}
	defer b.connections[masterId].Return(conn)

	m, err := conn.GetBulk(vb, keys)
	switch err.(type) {
	default:
		ch <- m
	case *gomemcached.MCResponse:
		fmt.Printf("Got a memcached error")
		st := err.(*gomemcached.MCResponse).Status
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
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.SetRaw(k, exp, data)
}

// Set a value in this bucket.
// The value will be stored as raw bytes.
func (b *Bucket) SetRaw(k string, exp int, v []byte) error {
	return b.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.Set(vb, k, 0, exp, v)
		if err != nil {
			return err
		}
		return nil
	})
}

// Adds a value to this bucket; like Set except that nothing happens
// if the key exists.  The value will be serialized into a JSON
// document.
func (b *Bucket) Add(k string, exp int, v interface{}) (added bool, err error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	return b.AddRaw(k, exp, data)
}

// Adds a value to this bucket; like SetRaw except that nothing
// happens if the key exists.  The value will be stored as raw bytes.
func (b *Bucket) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		switch res, err := memcached.UnwrapMemcachedError(mc.Add(vb, k, 0, exp, v)); {
		case err != nil:
			return err
		case res.Status == gomemcached.SUCCESS:
			added = true
		case res.Status != gomemcached.KEY_EEXISTS:
			return res
		}
		return nil
	})
	return
}

// Get a raw value from this bucket, including its CAS counter.
func (b *Bucket) GetsRaw(k string, cas *uint64) ([]byte, error) {
	var data []byte
	err := b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k)
		if err != nil {
			return err
		}
		if cas != nil {
			*cas = res.Cas
		}
		data = res.Body
		return nil
	})
	return data, err
}

// Get a value from this bucket, including its CAS counter.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Gets(k string, rv interface{}, cas *uint64) error {
	data, err := b.GetsRaw(k, cas)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, rv)
}

// Get a value from this bucket.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Get(k string, rv interface{}) error {
	return b.Gets(k, rv, nil)
}

// Get a raw value from this bucket.
func (b *Bucket) GetRaw(k string) ([]byte, error) {
	return b.GetsRaw(k, nil)
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string) error {
	return b.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.Del(vb, k)
		if err != nil {
			return err
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

// A callback function to update a document
type UpdateFunc func(current []byte) (updated []byte, err error)

// Return this as the error from an UpdateFunc to cancel the Update
// operation.
const UpdateCancel = memcached.CASQuit

// Safe update of a document, avoiding conflicts by using CAS.
//
// The callback function will be invoked with the current raw document
// contents (or nil if the document doesn't exist); it should return
// the updated raw contents (or nil to delete.)  If it decides not to
// change anything it can return UpdateCancel as the error.
//
// If another writer modifies the document between the get and the
// set, the callback will be invoked again with the newer value.
func (b *Bucket) Update(k string, exp int, callback UpdateFunc) error {
	return b.Do(k, func(mc *memcached.Client, vb uint16) error {
		var callbackErr error
		casFunc := func(current []byte) ([]byte, memcached.CasOp) {
			var updated []byte
			updated, callbackErr = callback(current)
			if callbackErr != nil {
				return nil, memcached.CASQuit
			} else if updated == nil {
				return updated, memcached.CASDelete
			} else {
				return updated, memcached.CASStore
			}
		}
		_, err := mc.CAS(vb, k, casFunc, exp)
		if err == memcached.CASQuit {
			err = callbackErr
		}
		return err
	})
}

// Install a design document.
func (b *Bucket) PutDDoc(docname string, value interface{}) error {
	u, err := b.randomBaseURL()
	if err != nil {
		return err
	}

	j, err := json.Marshal(value)
	if err != nil {
		return err
	}

	u.Path = fmt.Sprintf("/%s/_design/%s", b.Name, docname)
	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(j))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 201 {
		body, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Error installing view: %v / %s",
			res.Status, body)
	}

	return nil
}
