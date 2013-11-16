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

in any case, you can specify authentication credentials using
standard URL userinfo syntax:

 b, err := couchbase.GetBucket("http://bucketname:bucketpass@myserver:8091/",
         "default", "bucket")
*/
package couchbase

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

// Maximum number of times to retry a chunk of a bulk get on error.
var MaxBulkRetries = 1000

// Execute a function on a memcached connection to the node owning key "k"
//
// Note that this automatically handles transient errors by replaying
// your function on a "not-my-vbucket" error, so don't assume
// your command will only be executed only once.
func (b *Bucket) Do(k string, f func(mc *memcached.Client, vb uint16) error) error {
	vb := b.VBHash(k)
	maxTries := len(b.Nodes()) * 2
	for i := 0; i < maxTries; i++ {
		// We encapsulate the attempt within an anonymous function to allow
		// "defer" statement to work as intended.
		retry, err := func() (retry bool, err error) {
			vbm := b.VBServerMap()
			masterId := vbm.VBucketMap[vb][0]
			pool := b.getConnPool(masterId)
			conn, err := pool.Get()
			defer pool.Return(conn)
			if err != nil {
				return
			}

			err = f(conn, uint16(vb))
			if i, ok := err.(*gomemcached.MCResponse); ok {
				st := i.Status
				retry = st == gomemcached.NOT_MY_VBUCKET
			}
			return
		}()

		if retry {
			b.refresh()
		} else {
			return err
		}
	}

	return fmt.Errorf("Unable to complete action after %v attemps",
		maxTries)
}

type gathered_stats struct {
	sn   string
	vals map[string]string
}

func getStatsParallel(b *Bucket, offset int, which string,
	ch chan<- gathered_stats) {
	sn := b.VBServerMap().ServerList[offset]

	results := map[string]string{}
	pool := b.getConnPool(offset)
	conn, err := pool.Get()
	defer pool.Return(conn)
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

	vsm := b.VBServerMap()
	if vsm.ServerList == nil {
		return rv
	}
	// Go grab all the things at once.
	todo := len(vsm.ServerList)
	ch := make(chan gathered_stats, todo)

	for offset := range vsm.ServerList {
		go getStatsParallel(b, offset, which, ch)
	}

	// Gather the results
	for i := 0; i < len(vsm.ServerList); i++ {
		g := <-ch
		if len(g.vals) > 0 {
			rv[g.sn] = g.vals
		}
	}

	return rv
}

// Errors that are not considered fatal for our fetch loop
func isConnError(err error) bool {
	if err == io.EOF {
		return true
	}
	estr := err.Error()
	return strings.Contains(estr, "broken pipe") ||
		strings.Contains(estr, "connection reset")
}

func (b *Bucket) doBulkGet(vb uint16, keys []string,
	ch chan<- map[string]*gomemcached.MCResponse, ech chan error) {

	rv := map[string]*gomemcached.MCResponse{}

	attempts := 0
	done := false
	for attempts < MaxBulkRetries && !done {
		masterId := b.VBServerMap().VBucketMap[vb][0]
		attempts++

		// This stack frame exists to ensure we can clean up
		// connection at a reasonable time.
		err := func() error {
			pool := b.getConnPool(masterId)
			conn, err := pool.Get()
			if err != nil {
				// retry
				return nil
			}
			defer pool.Return(conn)

			m, err := conn.GetBulk(vb, keys)
			switch err.(type) {
			case *gomemcached.MCResponse:
				st := err.(*gomemcached.MCResponse).Status
				if st == gomemcached.NOT_MY_VBUCKET {
					b.refresh()
					// retry
					err = nil
				}
				return err
			case error:
				if !isConnError(err) {
					ech <- err
					ch <- rv
					return err
				}
				// retry
				return nil
			}
			if m != nil {
				if len(rv) == 0 {
					rv = m
				} else {
					for k, v := range m {
						rv[k] = v
					}
				}
			}
			done = true
			return nil
		}()

		if err != nil {
			return
		}
	}

	if attempts == MaxBulkRetries {
		ech <- fmt.Errorf("BulkGet exceeded MaxBulkRetries for vbucket %d", vb)
	}

	ch <- rv
}

func (b *Bucket) processBulkGet(kdm map[uint16][]string,
	ch chan map[string]*gomemcached.MCResponse, ech chan error) {
	wch := make(chan uint16)
	defer close(ch)
	defer close(ech)

	wg := &sync.WaitGroup{}
	worker := func() {
		defer wg.Done()
		for k := range wch {
			b.doBulkGet(k, kdm[k], ch, ech)
		}
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go worker()
	}

	for k := range kdm {
		wch <- k
	}
	close(wch)
	wg.Wait()
}

type multiError []error

func (m multiError) Error() string {
	if len(m) == 0 {
		panic("Error of none")
	}

	return fmt.Sprintf("{%v errors, starting with %v}", len(m), m[0].Error())
}

// Convert a stream of errors from ech into a multiError (or nil) and
// send down eout.
//
// At least one send is guaranteed on eout, but two is possible, so
// buffer the out channel appropriately.
func errorCollector(ech <-chan error, eout chan<- error) {
	defer func() { eout <- nil }()
	var errs multiError
	for e := range ech {
		errs = append(errs, e)
	}

	if len(errs) > 0 {
		eout <- errs
	}
}

func (b *Bucket) GetBulk(keys []string) (map[string]*gomemcached.MCResponse, error) {
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

	eout := make(chan error, 2)

	// processBulkGet will own both of these channels and
	// guarantee they're closed before it returns.
	ch := make(chan map[string]*gomemcached.MCResponse)
	ech := make(chan error)
	go b.processBulkGet(kdm, ch, ech)

	go errorCollector(ech, eout)

	rv := map[string]*gomemcached.MCResponse{}
	for m := range ch {
		for k, v := range m {
			rv[k] = v
		}
	}

	return rv, <-eout
}

// A set of option flags for the Write method.
type WriteOptions int

const (
	// If set, value is raw []byte or nil; don't JSON-encode it.
	Raw = WriteOptions(1 << iota)
	// If set, Write fails with ErrKeyExists if key already has a value.
	AddOnly
	// If set, Write will wait until the value is written to disk.
	Persist
	// If set, Write will wait until the value is available to be indexed by views.
	// (In Couchbase Server 2.x, this has the same effect as the Persist flag.)
	Indexable
	// If set, data is appended to existing value instead of replacing it.
	Append
)

// Error returned from Write with AddOnly flag, when key already exists in the bucket.
var ErrKeyExists = errors.New("Key exists")

// General-purpose value setter.
//
// The Set, Add and Delete methods are just wrappers around this.  The
// interpretation of `v` depends on whether the `Raw` option is
// given. If it is, v must be a byte array or nil. (A nil value causes
// a delete.) If `Raw` is not given, `v` will be marshaled as JSON
// before being written. It must be JSON-marshalable and it must not
// be nil.
func (b *Bucket) Write(k string, flags, exp int, v interface{},
	opt WriteOptions) (err error) {

	var data []byte
	if opt&Raw == 0 {
		data, err = json.Marshal(v)
		if err != nil {
			return err
		}
	} else if v != nil {
		data = v.([]byte)
	}

	var res *gomemcached.MCResponse
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		if opt&AddOnly != 0 {
			res, err = memcached.UnwrapMemcachedError(
				mc.Add(vb, k, flags, exp, data))
			if err == nil && res.Status != gomemcached.SUCCESS {
				if res.Status == gomemcached.KEY_EEXISTS {
					err = ErrKeyExists
				} else {
					err = res
				}
			}
		} else if opt&Append != 0 {
			res, err = mc.Append(vb, k, data)
		} else if data == nil {
			res, err = mc.Del(vb, k)
		} else {
			res, err = mc.Set(vb, k, flags, exp, data)
		}
		return err
	})

	if err == nil && (opt&(Persist|Indexable) != 0) {
		err = b.WaitForPersistence(k, res.Cas, data == nil)
	}

	return err
}

// Set a value in this bucket.
// The value will be serialized into a JSON document.
func (b *Bucket) Set(k string, exp int, v interface{}) error {
	return b.Write(k, 0, exp, v, 0)
}

// Set a value in this bucket.
// The value will be stored as raw bytes.
func (b *Bucket) SetRaw(k string, exp int, v []byte) error {
	return b.Write(k, 0, exp, v, Raw)
}

// Adds a value to this bucket; like Set except that nothing happens
// if the key exists.  The value will be serialized into a JSON
// document.
func (b *Bucket) Add(k string, exp int, v interface{}) (added bool, err error) {
	err = b.Write(k, 0, exp, v, AddOnly)
	if err == ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

// Adds a value to this bucket; like SetRaw except that nothing
// happens if the key exists.  The value will be stored as raw bytes.
func (b *Bucket) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	err = b.Write(k, 0, exp, v, AddOnly|Raw)
	if err == ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

func (b *Bucket) Append(k string, data []byte) error {
	return b.Write(k, 0, 0, data, Append|Raw)
}

// Get a raw value from this bucket, including its CAS counter and flags.
func (b *Bucket) GetsRaw(k string) (data []byte, flags int,
	cas uint64, err error) {

	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k)
		if err != nil {
			return err
		}
		cas = res.Cas
		if len(res.Extras) >= 4 {
			flags = int(binary.BigEndian.Uint32(res.Extras))
		}
		data = res.Body
		return nil
	})
	return
}

// Get a value from this bucket, including its CAS counter.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Gets(k string, rv interface{}, caso *uint64) error {
	data, _, cas, err := b.GetsRaw(k)
	if err != nil {
		return err
	}
	if caso != nil {
		*caso = cas
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
	d, _, _, err := b.GetsRaw(k)
	return d, err
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string) error {
	return b.Write(k, 0, 0, nil, Raw)
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

// Wrapper around memcached.CASNext()
func (b *Bucket) casNext(k string, exp int, state *memcached.CASState) bool {
	keepGoing := false
	state.Err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		keepGoing = mc.CASNext(vb, k, exp, state)
		return state.Err
	})
	return keepGoing && state.Err == nil
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
	_, err := b.update(k, exp, callback)
	return err
}

// internal version of Update that returns a CAS value
func (b *Bucket) update(k string, exp int, callback UpdateFunc) (newCas uint64, err error) {
	var state memcached.CASState
	for b.casNext(k, exp, &state) {
		var err error
		if state.Value, err = callback(state.Value); err != nil {
			return 0, err
		}
	}
	return state.Cas, state.Err
}

// A callback function to update a document
type WriteUpdateFunc func(current []byte) (updated []byte, opt WriteOptions, err error)

// Safe update of a document, avoiding conflicts by using CAS.
// WriteUpdate is like Update, except that the callback can return a set of WriteOptions,
// of which Persist and Indexable are recognized: these cause the call to wait until the
// document update has been persisted to disk and/or become available to index.
func (b *Bucket) WriteUpdate(k string, exp int, callback WriteUpdateFunc) error {
	var writeOpts WriteOptions
	var deletion bool
	// Wrap the callback in an UpdateFunc we can pass to Update:
	updateCallback := func(current []byte) (updated []byte, err error) {
		update, opt, err := callback(current)
		writeOpts = opt
		deletion = (update == nil)
		return update, err
	}
	cas, err := b.update(k, exp, updateCallback)
	if err != nil {
		return err
	}
	// If callback asked, wait for persistence or indexability:
	if writeOpts&(Persist|Indexable) != 0 {
		err = b.WaitForPersistence(k, cas, deletion)
	}
	return err
}

// Observes the current state of a document.
func (b *Bucket) Observe(k string) (result memcached.ObserveResult, err error) {
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		result, err = mc.Observe(vb, k)
		return err
	})
	return
}

// Returned from WaitForPersistence (or Write, if the Persistent or Indexable flag is used)
// if the value has been overwritten by another before being persisted.
var ErrOverwritten = errors.New("Overwritten")

// Returned from WaitForPersistence (or Write, if the Persistent or Indexable flag is used)
// if the value hasn't been persisted by the timeout interval
var ErrTimeout = errors.New("Timeout")

// WaitForPersistence waits for an item to be considered durable.
//
// Besides transport errors, ErrOverwritten may be returned if the
// item is overwritten before it reaches durability.  ErrTimeout may
// occur if the item isn't found durable in a reasonable amount of
// time.
func (b *Bucket) WaitForPersistence(k string, cas uint64, deletion bool) error {
	timeout := 10 * time.Second
	sleepDelay := 5 * time.Millisecond
	start := time.Now()
	for {
		time.Sleep(sleepDelay)
		sleepDelay += sleepDelay / 2 // multiply delay by 1.5 every time

		result, err := b.Observe(k)
		if err != nil {
			return err
		}
		if persisted, overwritten := result.CheckPersistence(cas, deletion); overwritten {
			return ErrOverwritten
		} else if persisted {
			return nil
		}

		if result.PersistenceTime > 0 {
			timeout = 2 * result.PersistenceTime
		}
		if time.Since(start) >= timeout-sleepDelay {
			return ErrTimeout
		}
	}
}
