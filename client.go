/*
Package couchbase provides a smart client for go.

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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client" // package name is 'memcached'
	"github.com/couchbase/goutils/logging"
)

// Mutation Token
type MutationToken struct {
	VBid  uint16 // vbucket id
	Guard uint64 // vbuuid
	Value uint64 // sequence number
}

// Maximum number of times to retry a chunk of a bulk get on error.
var MaxBulkRetries = 5000
var backOffDuration time.Duration = 100 * time.Millisecond
var MaxBackOffRetries = 25 // exponentail backOff result in over 30sec (25*13*0.1s)

// If this is set to a nonzero duration, Do() and ViewCustom() will log a warning if the call
// takes longer than that.
var SlowServerCallWarningThreshold time.Duration

func slowLog(startTime time.Time, format string, args ...interface{}) {
	if elapsed := time.Now().Sub(startTime); elapsed > SlowServerCallWarningThreshold {
		pc, _, _, _ := runtime.Caller(2)
		caller := runtime.FuncForPC(pc).Name()
		logging.Infof("go-couchbase: "+format+" in "+caller+" took "+elapsed.String(), args...)
	}
}

// Return true if error is KEY_EEXISTS. Required by cbq-engine
func IsKeyEExistsError(err error) bool {

	res, ok := err.(*gomemcached.MCResponse)
	if ok && res.Status == gomemcached.KEY_EEXISTS {
		return true
	}

	return false
}

// Return true if error is KEY_ENOENT. Required by cbq-engine
func IsKeyNoEntError(err error) bool {

	res, ok := err.(*gomemcached.MCResponse)
	if ok && res.Status == gomemcached.KEY_ENOENT {
		return true
	}

	return false
}

// Return true if error suggests a bucket refresh is required. Required by cbq-engine
func IsRefreshRequired(err error) bool {

	res, ok := err.(*gomemcached.MCResponse)
	if ok && (res.Status == gomemcached.NO_BUCKET || res.Status == gomemcached.NOT_MY_VBUCKET) {
		return true
	}

	return false
}

// Return true if a collection is not known. Required by cbq-engine
func IsUnknownCollection(err error) bool {

	res, ok := err.(*gomemcached.MCResponse)
	if ok && (res.Status == gomemcached.UNKNOWN_COLLECTION) {
		return true
	}

	return false
}

// ClientOpCallback is called for each invocation of Do.
var ClientOpCallback func(opname, k string, start time.Time, err error)

// Do executes a function on a memcached connection to the node owning key "k"
//
// Note that this automatically handles transient errors by replaying
// your function on a "not-my-vbucket" error, so don't assume
// your command will only be executed only once.
func (b *Bucket) Do(k string, f func(mc *memcached.Client, vb uint16) error) (err error) {
	return b.Do2(k, f, true)
}

func (b *Bucket) Do2(k string, f func(mc *memcached.Client, vb uint16) error, deadline bool) (err error) {
	var lastError error

	if SlowServerCallWarningThreshold > 0 {
		defer slowLog(time.Now(), "call to Do(%q)", k)
	}

	vb := b.VBHash(k)
	maxTries := len(b.Nodes()) * 2
	for i := 0; i < maxTries; i++ {
		conn, pool, err := b.getConnectionToVBucket(vb)
		if err != nil {
			if (err == errNoPool || isConnError(err)) && backOff(i, maxTries, backOffDuration, true) {
				b.Refresh()
				continue
			}
			return err
		}

		if deadline && DefaultTimeout > 0 {
			conn.SetDeadline(getDeadline(noDeadline, DefaultTimeout))
		} else {
			conn.SetDeadline(noDeadline)
		}
		lastError = f(conn, uint16(vb))

		retry := false
		discard := isOutOfBoundsError(lastError) || IsReadTimeOutError(lastError)

		// MB-30967 / MB-31001 implement back off for transient errors
		if resp, ok := lastError.(*gomemcached.MCResponse); ok {
			switch resp.Status {
			case gomemcached.NOT_MY_VBUCKET:
				retry = backOff(i, maxTries, backOffDuration, false)
				if retry {
					b.Refresh()
				}

				// MB-28842: in case of NMVB, check if the node is still part of the map
				// and ditch the connection if it isn't.
				discard = b.checkVBmap(pool.Node())
			case gomemcached.NOT_SUPPORTED:
				discard = true
				retry = true
			case gomemcached.ENOMEM:
				fallthrough
			case gomemcached.TMPFAIL, gomemcached.EBUSY:
				retry = backOff(i, maxTries, backOffDuration, true)
			}
		} else if lastError != nil && isConnError(lastError) && backOff(i, maxTries, backOffDuration, true) {
			retry = true
		}

		if discard {
			pool.Discard(conn)
		} else {
			pool.Return(conn)
		}

		if !retry {
			return lastError
		}
	}

	if resp, ok := lastError.(*gomemcached.MCResponse); ok {
		err := gomemcached.StatusNames[resp.Status]
		if err == "" {
			err = fmt.Sprintf("KV status %v", resp.Status)
		}
		return fmt.Errorf("unable to complete action after %v attempts: %v", maxTries, err)
	} else {
		return fmt.Errorf("unable to complete action after %v attempts: %v", maxTries, lastError)
	}
}

type GatheredStats struct {
	Server string
	Stats  map[string]string
	Err    error
}

func getStatsParallel(sn string, b *Bucket, offset int, which string,
	ch chan<- GatheredStats) {
	pool := b.getConnPool(offset)
	var gatheredStats GatheredStats

	conn, err := pool.Get()
	defer func() {
		pool.Return(conn)
		ch <- gatheredStats
	}()

	if err != nil {
		gatheredStats = GatheredStats{Server: sn, Err: err}
	} else {
		conn.SetDeadline(getDeadline(time.Time{}, DefaultTimeout))
		sm, err := conn.StatsMap(which)
		gatheredStats = GatheredStats{Server: sn, Stats: sm, Err: err}
	}
}

func getStatsParallelFunc(fn func(key, val []byte), sn string, b *Bucket, offset int, which string,
	ch chan<- GatheredStats) {
	pool := b.getConnPool(offset)

	conn, err := pool.Get()

	if err == nil {
		conn.SetDeadline(getDeadline(time.Time{}, DefaultTimeout))
		err = conn.StatsFunc(which, fn)
		pool.Return(conn)
	}
	ch <- GatheredStats{Server: sn, Err: err}
}

// GetStats gets a set of stats from all servers.
//
// Returns a map of server ID -> map of stat key to map value.
func (b *Bucket) GetStats(which string) map[string]map[string]string {
	rv := map[string]map[string]string{}
	for server, gs := range b.GatherStats(which) {
		if len(gs.Stats) > 0 {
			rv[server] = gs.Stats
		}
	}
	return rv
}

// GatherStats returns a map of server ID -> GatheredStats from all servers.
func (b *Bucket) GatherStats(which string) map[string]GatheredStats {
	vsm := b.VBServerMap()
	if vsm.ServerList == nil {
		return nil
	}

	// Go grab all the things at once.
	ch := make(chan GatheredStats, len(vsm.ServerList))
	for i, sn := range vsm.ServerList {
		go getStatsParallel(sn, b, i, which, ch)
	}

	// Gather the results
	rv := map[string]GatheredStats{}
	for range vsm.ServerList {
		gs := <-ch
		rv[gs.Server] = gs
	}
	return rv
}

// GatherStats returns a map of server ID -> GatheredStats from all servers.
func (b *Bucket) GatherStatsFunc(which string, fn func(key, val []byte)) map[string]error {
	var errMap map[string]error

	vsm := b.VBServerMap()
	if vsm.ServerList == nil {
		return errMap
	}

	// Go grab all the things at once.
	ch := make(chan GatheredStats, len(vsm.ServerList))
	for i, sn := range vsm.ServerList {
		go getStatsParallelFunc(fn, sn, b, i, which, ch)
	}

	// Gather the results
	for range vsm.ServerList {
		gs := <-ch
		if gs.Err != nil {
			if errMap == nil {
				errMap = make(map[string]error)
				errMap[gs.Server] = gs.Err
			}
		}
	}
	return errMap
}

type BucketStats int

const (
	StatCount = BucketStats(iota)
	StatSize
)

var bucketStatString = []string{
	"curr_items",
	"ep_value_size",
}

var collectionStatString = []string{
	"items",
	"data_size",
}

// Get selected bucket or collection stats
func (b *Bucket) GetIntStats(refresh bool, which []BucketStats, context ...*memcached.ClientContext) ([]int64, error) {
	if refresh {
		b.Refresh()
	}

	var vals []int64 = make([]int64, len(which))
	if len(vals) == 0 {
		return vals, nil
	}

	var outErr error
	if len(context) > 0 {

		collKey := fmt.Sprintf("collections-byid 0x%x", context[0].CollId)
		errs := b.GatherStatsFunc(collKey, func(key, val []byte) {
			for i, f := range which {
				lk := len(key)
				ls := len(collectionStatString[f])
				if lk >= ls && string(key[lk-ls:]) == collectionStatString[f] {
					v, err := strconv.ParseInt(string(val), 10, 64)
					if err == nil {
						atomic.AddInt64(&vals[i], v)
					} else if outErr == nil {
						outErr = err
					}
				}
			}
		})

		// have to use a range to access any one element of a map
		for _, err := range errs {
			return nil, err
		}
	} else {
		errs := b.GatherStatsFunc("", func(key, val []byte) {
			for i, f := range which {
				if string(key) == bucketStatString[f] {
					v, err := strconv.ParseInt(string(val), 10, 64)
					if err == nil {
						atomic.AddInt64(&vals[i], v)
					} else if outErr == nil {
						outErr = err
					}
				}
			}
		})

		// have to use a range to access any one element of a map
		for _, err := range errs {
			return nil, err
		}
	}

	return vals, outErr
}

// Get bucket count through the bucket stats
func (b *Bucket) GetCount(refresh bool, context ...*memcached.ClientContext) (count int64, err error) {
	if refresh {
		b.Refresh()
	}

	var cnt int64
	if len(context) > 0 {
		key := fmt.Sprintf("collections-byid 0x%x", context[0].CollId)
		resKey := ""
		for _, gs := range b.GatherStats(key) {
			if len(gs.Stats) > 0 {

				// the key encodes the scope and collection id
				// we don't have the scope id, so we have to find it...
				if resKey == "" {
					for k, _ := range gs.Stats {
						resKey = strings.TrimRightFunc(k, func(r rune) bool {
							return r != ':'
						}) + "items"
						break
					}
				}
				cnt, err = strconv.ParseInt(gs.Stats[resKey], 10, 64)
				if err != nil {
					return 0, err
				}
				count += cnt
			} else if gs.Err != nil {
				return 0, gs.Err
			}
		}
	} else {
		for _, gs := range b.GatherStats("") {
			if len(gs.Stats) > 0 {
				cnt, err = strconv.ParseInt(gs.Stats["curr_items"], 10, 64)
				if err != nil {
					return 0, err
				}
				count += cnt
			} else if gs.Err != nil {
				return 0, gs.Err
			}
		}
	}

	return count, nil
}

// Get bucket document size through the bucket stats
func (b *Bucket) GetSize(refresh bool, context ...*memcached.ClientContext) (size int64, err error) {

	if refresh {
		b.Refresh()
	}

	var sz int64
	if len(context) > 0 {
		key := fmt.Sprintf("collections-byid 0x%x", context[0].CollId)
		resKey := ""
		for _, gs := range b.GatherStats(key) {
			if len(gs.Stats) > 0 {

				// the key encodes the scope and collection id
				// we don't have the scope id, so we have to find it...
				if resKey == "" {
					for k, _ := range gs.Stats {
						resKey = strings.TrimRightFunc(k, func(r rune) bool {
							return r != ':'
						}) + "data_size"
						break
					}
				}
				sz, err = strconv.ParseInt(gs.Stats[resKey], 10, 64)
				if err != nil {
					return 0, err
				}
				size += sz
			} else if gs.Err != nil {
				return 0, gs.Err
			}
		}
	} else {
		for _, gs := range b.GatherStats("") {
			if len(gs.Stats) > 0 {
				sz, err = strconv.ParseInt(gs.Stats["ep_value_size"], 10, 64)
				if err != nil {
					return 0, err
				}
				size += sz
			} else if gs.Err != nil {
				return 0, gs.Err
			}
		}
	}

	return size, nil
}

func isAuthError(err error) bool {
	estr := err.Error()
	return strings.Contains(estr, "Auth failure")
}

func IsReadTimeOutError(err error) bool {
	if err == nil {
		return false
	}
	estr := err.Error()
	return strings.Contains(estr, "read tcp") ||
		strings.Contains(estr, "i/o timeout")
}

func isTimeoutError(err error) bool {
	estr := err.Error()
	return strings.Contains(estr, "i/o timeout") ||
		strings.Contains(estr, "connection timed out") ||
		strings.Contains(estr, "no route to host")
}

// Errors that are not considered fatal for our fetch loop
func isConnError(err error) bool {
	if err == io.EOF {
		return true
	}
	estr := err.Error()
	return strings.Contains(estr, "broken pipe") ||
		strings.Contains(estr, "connection reset") ||
		strings.Contains(estr, "connection refused") ||
		strings.Contains(estr, "connection pool is closed")
}

func isOutOfBoundsError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Out of Bounds error")

}

func isAddrNotAvailable(err error) bool {
	if err == nil {
		return false
	}
	estr := err.Error()
	return strings.Contains(estr, "cannot assign requested address")
}

func getDeadline(reqDeadline time.Time, duration time.Duration) time.Time {
	if reqDeadline.IsZero() {
		if duration > 0 {
			return time.Unix(time.Now().Unix(), 0).Add(duration)
		} else {
			return noDeadline
		}
	}
	return reqDeadline
}

func backOff(attempt, maxAttempts int, duration time.Duration, exponential bool) bool {
	if attempt < maxAttempts {
		// 0th attempt return immediately
		if attempt > 0 {
			if exponential {
				duration = time.Duration(attempt) * duration
			}
			time.Sleep(duration)
		}
		return true
	}

	return false
}

func (b *Bucket) doBulkGet(vb uint16, keys []string, reqDeadline time.Time,
	ch chan<- map[string]*gomemcached.MCResponse, ech chan<- error, subPaths []string,
	eStatus *errorStatus, context ...*memcached.ClientContext) {
	if SlowServerCallWarningThreshold > 0 {
		defer slowLog(time.Now(), "call to doBulkGet(%d, %d keys)", vb, len(keys))
	}

	rv := _STRING_MCRESPONSE_POOL.Get()
	attempts := 0
	backOffAttempts := 0
	done := false
	bname := b.Name
	var lastError error
	for ; attempts < MaxBulkRetries && !done && !eStatus.errStatus; attempts++ {

		if len(b.VBServerMap().VBucketMap) < int(vb) {
			//fatal
			err := fmt.Errorf("vbmap smaller than requested for %v", bname)
			logging.Errorf("go-couchbase: %v vb %d vbmap len %d", err.Error(), vb, len(b.VBServerMap().VBucketMap))
			ech <- err
			return
		}

		masterID := b.VBServerMap().VBucketMap[vb][0]

		if masterID < 0 {
			// fatal
			err := fmt.Errorf("No master node available for %v vb %d", bname, vb)
			logging.Errorf("%v", err.Error())
			ech <- err
			return
		}

		// This stack frame exists to ensure we can clean up
		// connection at a reasonable time.
		err := func() error {
			pool := b.getConnPool(masterID)
			conn, err := pool.Get()
			if err != nil {
				if isAuthError(err) || isTimeoutError(err) {
					logging.Errorf("Fatal Error %v : %v", bname, err)
					ech <- err
					return err
				} else if isConnError(err) {
					if !backOff(backOffAttempts, MaxBackOffRetries, backOffDuration, true) {
						logging.Errorf("Connection Error %v : %v", bname, err)
						ech <- err
						return err
					}
					b.Refresh()
					backOffAttempts++
				} else if err == errNoPool {
					if !backOff(backOffAttempts, MaxBackOffRetries, backOffDuration, true) {
						logging.Errorf("Connection Error %v : %v", bname, err)
						ech <- err
						return err
					}
					err = b.Refresh()
					if err != nil {
						ech <- err
						return err
					}
					backOffAttempts++

					// retry, and make no noise
					return nil
				} else if isAddrNotAvailable(err) {
					if !backOff(backOffAttempts, MaxBackOffRetries, backOffDuration, true) {
						logging.Errorf("Out of ephemeral ports: %v : %v", bname, err)
						ech <- err
						return err
					}
					b.Refresh()
					backOffAttempts++
				}
				if lastError == nil || err.Error() != lastError.Error() || MaxBulkRetries-1 == attempts {
					if lastError != nil {
						logging.Infof("(... attempt: %v) Pool Get returned %v: %v", attempts-1, bname, err)
					}
					logging.Infof("(Attempt: %v) Pool Get returned %v: %v", attempts, bname, err)
					lastError = err
				}
				// retry
				return nil
			}
			lastError = nil

			conn.SetDeadline(getDeadline(reqDeadline, DefaultTimeout))
			err = conn.GetBulk(vb, keys, rv, subPaths, context...)

			discard := false
			defer func() {
				if discard {
					pool.Discard(conn)
				} else {
					pool.Return(conn)
				}
			}()

			switch err.(type) {
			case *gomemcached.MCResponse:
				notSMaxTries := len(b.Nodes()) * 2
				st := err.(*gomemcached.MCResponse).Status
				if st == gomemcached.NOT_MY_VBUCKET {

					// increment first, as we want a delay
					backOffAttempts++
					backOff(backOffAttempts, MaxBackOffRetries, backOffDuration, false)
					b.Refresh()
					discard = b.checkVBmap(pool.Node())
					return nil // retry
				} else if st == gomemcached.NOT_SUPPORTED && attempts < notSMaxTries {
					b.Refresh()
					discard = b.checkVBmap(pool.Node())
					return nil // retry
				} else if st == gomemcached.EBUSY || st == gomemcached.LOCKED {
					if (attempts % (MaxBulkRetries / 100)) == 0 {
						logging.Infof("Retrying Memcached error (%v) FOR %v(vbid:%d, keys:<ud>%v</ud>)",
							err.Error(), bname, vb, keys)
					}
					return nil // retry
				} else if (st == gomemcached.ENOMEM || st == gomemcached.TMPFAIL) && backOff(backOffAttempts, MaxBackOffRetries, backOffDuration, true) {
					// MB-30967 / MB-31001 use backoff for TMPFAIL too
					backOffAttempts++
					logging.Infof("Retrying Memcached error (%v) FOR %v(vbid:%d, keys:<ud>%v</ud>)",
						err.Error(), bname, vb, keys)
					return nil // retry
				}
				ech <- err
				return err
			case error:
				if isOutOfBoundsError(err) {
					// We got an out of bounds error or a read timeout error; retry the operation
					discard = true
					return nil
				} else if isConnError(err) && backOff(backOffAttempts, MaxBackOffRetries, backOffDuration, true) {
					backOffAttempts++
					logging.Errorf("Connection Error: %s. Refreshing bucket %v (vbid:%v,keys:<ud>%v</ud>)",
						err.Error(), bname, vb, keys)
					discard = true
					b.Refresh()
					return nil // retry
				} else if IsReadTimeOutError(err) {
					discard = true
					logging.Errorf("Attempt %v: Terminating bulk request on timeout.", attempts)
				}
				ech <- err
				ch <- rv
				return err
			}

			done = true
			return nil
		}()

		if err != nil {
			return
		}
	}

	if attempts >= MaxBulkRetries {
		err := fmt.Errorf("bulkget exceeded MaxBulkRetries for %v(vbid:%d,keys:<ud>%v</ud>)", bname, vb, keys)
		logging.Errorf("%v", err.Error())
		ech <- err
	}

	ch <- rv
}

type errorStatus struct {
	errStatus bool
}

type vbBulkGet struct {
	b           *Bucket
	ch          chan<- map[string]*gomemcached.MCResponse
	ech         chan<- error
	k           uint16
	keys        []string
	reqDeadline time.Time
	wg          *sync.WaitGroup
	subPaths    []string
	groupError  *errorStatus
	context     []*memcached.ClientContext
}

const _NUM_CHANNELS = 5

var _NUM_CHANNEL_WORKERS = (runtime.NumCPU() + 1) / 2
var DefaultDialTimeout = time.Duration(0)
var DefaultTimeout = time.Duration(0)
var noDeadline = time.Time{}

// Buffer 4k requests per worker
var _VB_BULK_GET_CHANNELS []chan *vbBulkGet

func InitBulkGet() {

	DefaultDialTimeout = 20 * time.Second
	DefaultTimeout = 120 * time.Second

	memcached.SetDefaultDialTimeout(DefaultDialTimeout)

	_VB_BULK_GET_CHANNELS = make([]chan *vbBulkGet, _NUM_CHANNELS)

	for i := 0; i < _NUM_CHANNELS; i++ {
		channel := make(chan *vbBulkGet, 16*1024*_NUM_CHANNEL_WORKERS)
		_VB_BULK_GET_CHANNELS[i] = channel

		for j := 0; j < _NUM_CHANNEL_WORKERS; j++ {
			go vbBulkGetWorker(channel)
		}
	}
}

func vbBulkGetWorker(ch chan *vbBulkGet) {
	defer func() {
		// Workers cannot panic and die
		recover()
		go vbBulkGetWorker(ch)
	}()

	for vbg := range ch {
		vbDoBulkGet(vbg)
	}
}

func vbDoBulkGet(vbg *vbBulkGet) {
	defer vbg.wg.Done()
	defer func() {
		// Workers cannot panic and die
		recover()
	}()
	vbg.b.doBulkGet(vbg.k, vbg.keys, vbg.reqDeadline, vbg.ch, vbg.ech, vbg.subPaths, vbg.groupError, vbg.context...)
}

var _ERR_CHAN_FULL = fmt.Errorf("Data request queue full, aborting query.")

func (b *Bucket) processBulkGet(kdm map[uint16][]string, reqDeadline time.Time,
	ch chan<- map[string]*gomemcached.MCResponse, ech chan<- error, subPaths []string,
	eStatus *errorStatus, context ...*memcached.ClientContext) {

	defer close(ch)
	defer close(ech)

	wg := &sync.WaitGroup{}

	for k, keys := range kdm {

		// GetBulk() group has error donot Queue items for this group
		if eStatus.errStatus {
			break
		}

		vbg := &vbBulkGet{
			b:           b,
			ch:          ch,
			ech:         ech,
			k:           k,
			keys:        keys,
			reqDeadline: reqDeadline,
			wg:          wg,
			subPaths:    subPaths,
			groupError:  eStatus,
			context:     context,
		}

		wg.Add(1)

		// Random int
		// Right shift to avoid 8-byte alignment, and take low bits
		c := (uintptr(unsafe.Pointer(vbg)) >> 4) % _NUM_CHANNELS

		select {
		case _VB_BULK_GET_CHANNELS[c] <- vbg:
			// No-op
		default:
			// Buffer full, abandon the bulk get
			ech <- _ERR_CHAN_FULL
			wg.Add(-1)
		}
	}

	// Wait for my vb bulk gets
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
func errorCollector(ech <-chan error, eout chan<- error, eStatus *errorStatus) {
	defer func() { eout <- nil }()
	var errs multiError
	for e := range ech {
		if !eStatus.errStatus && !IsKeyNoEntError(e) {
			eStatus.errStatus = true
		}

		errs = append(errs, e)
	}

	if len(errs) > 0 {
		eout <- errs
	}
}

// Fetches multiple keys concurrently, with []byte values
//
// This is a wrapper around GetBulk which converts all values returned
// by GetBulk from raw memcached responses into []byte slices.
// Returns one document for duplicate keys
func (b *Bucket) GetBulkRaw(keys []string, context ...*memcached.ClientContext) (map[string][]byte, error) {

	resp, eout := b.getBulk(keys, noDeadline, nil, context...)

	rv := make(map[string][]byte, len(keys))
	for k, av := range resp {
		rv[k] = av.Body
	}

	b.ReleaseGetBulkPools(resp)
	return rv, eout

}

// GetBulk fetches multiple keys concurrently.
//
// Unlike more convenient GETs, the entire response is returned in the
// map array for each key.  Keys that were not found will not be included in
// the map.

func (b *Bucket) GetBulk(keys []string, reqDeadline time.Time, subPaths []string, context ...*memcached.ClientContext) (map[string]*gomemcached.MCResponse, error) {
	return b.getBulk(keys, reqDeadline, subPaths, context...)
}

func (b *Bucket) ReleaseGetBulkPools(rv map[string]*gomemcached.MCResponse) {
	_STRING_MCRESPONSE_POOL.Put(rv)
}

func (b *Bucket) getBulk(keys []string, reqDeadline time.Time, subPaths []string, context ...*memcached.ClientContext) (map[string]*gomemcached.MCResponse, error) {
	kdm := _VB_STRING_POOL.Get()
	defer _VB_STRING_POOL.Put(kdm)
	for _, k := range keys {
		if k != "" {
			vb := uint16(b.VBHash(k))
			a, ok1 := kdm[vb]
			if !ok1 {
				a = _STRING_POOL.Get()
			}
			kdm[vb] = append(a, k)
		}
	}

	eout := make(chan error, 2)
	groupErrorStatus := &errorStatus{}

	// processBulkGet will own both of these channels and
	// guarantee they're closed before it returns.
	ch := make(chan map[string]*gomemcached.MCResponse)
	ech := make(chan error)

	go errorCollector(ech, eout, groupErrorStatus)
	go b.processBulkGet(kdm, reqDeadline, ch, ech, subPaths, groupErrorStatus, context...)

	var rv map[string]*gomemcached.MCResponse

	for m := range ch {
		if rv == nil {
			rv = m
			continue
		}

		for k, v := range m {
			rv[k] = v
		}
		_STRING_MCRESPONSE_POOL.Put(m)
	}

	return rv, <-eout
}

// WriteOptions is the set of option flags availble for the Write
// method.  They are ORed together to specify the desired request.
type WriteOptions int

const (
	// Raw specifies that the value is raw []byte or nil; don't
	// JSON-encode it.
	Raw = WriteOptions(1 << iota)
	// AddOnly indicates an item should only be written if it
	// doesn't exist, otherwise ErrKeyExists is returned.
	AddOnly
	// Persist causes the operation to block until the server
	// confirms the item is persisted.
	Persist
	// Indexable causes the operation to block until it's availble via the index.
	Indexable
	// Append indicates the given value should be appended to the
	// existing value for the given key.
	Append
)

var optNames = []struct {
	opt  WriteOptions
	name string
}{
	{Raw, "raw"},
	{AddOnly, "addonly"}, {Persist, "persist"},
	{Indexable, "indexable"}, {Append, "append"},
}

// String representation of WriteOptions
func (w WriteOptions) String() string {
	f := []string{}
	for _, on := range optNames {
		if w&on.opt != 0 {
			f = append(f, on.name)
			w &= ^on.opt
		}
	}
	if len(f) == 0 || w != 0 {
		f = append(f, fmt.Sprintf("0x%x", int(w)))
	}
	return strings.Join(f, "|")
}

// Error returned from Write with AddOnly flag, when key already exists in the bucket.
var ErrKeyExists = errors.New("key exists")

// General-purpose value setter.
//
// The Set, Add and Delete methods are just wrappers around this.  The
// interpretation of `v` depends on whether the `Raw` option is
// given. If it is, v must be a byte array or nil. (A nil value causes
// a delete.) If `Raw` is not given, `v` will be marshaled as JSON
// before being written. It must be JSON-marshalable and it must not
// be nil.
func (b *Bucket) Write(k string, flags, exp int, v interface{},
	opt WriteOptions, context ...*memcached.ClientContext) (err error) {

	_, err = b.WriteWithCAS(k, flags, exp, v, opt, context...)

	return err
}

func (b *Bucket) WriteWithCAS(k string, flags, exp int, v interface{},
	opt WriteOptions, context ...*memcached.ClientContext) (cas uint64, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) {
			ClientOpCallback(fmt.Sprintf("Write(%v)", opt), k, t, err)
		}(time.Now())
	}

	var data []byte
	if opt&Raw == 0 {
		data, err = json.Marshal(v)
		if err != nil {
			return cas, err
		}
	} else if v != nil {
		data = v.([]byte)
	}

	var res *gomemcached.MCResponse
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		if opt&AddOnly != 0 {
			res, err = memcached.UnwrapMemcachedError(
				mc.Add(vb, k, flags, exp, data, context...))
			if err == nil && res.Status != gomemcached.SUCCESS {
				if res.Status == gomemcached.KEY_EEXISTS {
					err = ErrKeyExists
				} else {
					err = res
				}
			}
		} else if opt&Append != 0 {
			res, err = mc.Append(vb, k, data, context...)
		} else if data == nil {
			res, err = mc.Del(vb, k, context...)
		} else {
			res, err = mc.Set(vb, k, flags, exp, data, context...)
		}

		if err == nil {
			cas = res.Cas
		}

		return err
	})

	if err == nil && (opt&(Persist|Indexable) != 0) {
		err = b.WaitForPersistence(k, cas, data == nil)
	}

	return cas, err
}

func (b *Bucket) WriteWithMT(k string, flags, exp int, v interface{},
	opt WriteOptions, context ...*memcached.ClientContext) (mt *MutationToken, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) {
			ClientOpCallback(fmt.Sprintf("WriteWithMT(%v)", opt), k, t, err)
		}(time.Now())
	}

	var data []byte
	if opt&Raw == 0 {
		data, err = json.Marshal(v)
		if err != nil {
			return nil, err
		}
	} else if v != nil {
		data = v.([]byte)
	}

	var res *gomemcached.MCResponse
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		if opt&AddOnly != 0 {
			res, err = memcached.UnwrapMemcachedError(
				mc.Add(vb, k, flags, exp, data, context...))
			if err == nil && res.Status != gomemcached.SUCCESS {
				if res.Status == gomemcached.KEY_EEXISTS {
					err = ErrKeyExists
				} else {
					err = res
				}
			}
		} else if opt&Append != 0 {
			res, err = mc.Append(vb, k, data, context...)
		} else if data == nil {
			res, err = mc.Del(vb, k, context...)
		} else {
			res, err = mc.Set(vb, k, flags, exp, data, context...)
		}

		if len(res.Extras) >= 16 {
			vbuuid := uint64(binary.BigEndian.Uint64(res.Extras[0:8]))
			seqNo := uint64(binary.BigEndian.Uint64(res.Extras[8:16]))
			mt = &MutationToken{VBid: vb, Guard: vbuuid, Value: seqNo}
		}

		return err
	})

	if err == nil && (opt&(Persist|Indexable) != 0) {
		err = b.WaitForPersistence(k, res.Cas, data == nil)
	}

	return mt, err
}

// Set a value in this bucket with Cas and return the new Cas value
func (b *Bucket) Cas(k string, exp int, cas uint64, v interface{}, context ...*memcached.ClientContext) (uint64, error) {
	return b.WriteCas(k, 0, exp, cas, v, 0, context...)
}

// Set a value in this bucket with Cas without json encoding it
func (b *Bucket) CasRaw(k string, exp int, cas uint64, v interface{}, context ...*memcached.ClientContext) (uint64, error) {
	return b.WriteCas(k, 0, exp, cas, v, Raw, context...)
}

func (b *Bucket) WriteCas(k string, flags, exp int, cas uint64, v interface{},
	opt WriteOptions, context ...*memcached.ClientContext) (newCas uint64, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) {
			ClientOpCallback(fmt.Sprintf("Write(%v)", opt), k, t, err)
		}(time.Now())
	}

	var data []byte
	if opt&Raw == 0 {
		data, err = json.Marshal(v)
		if err != nil {
			return 0, err
		}
	} else if v != nil {
		data = v.([]byte)
	}

	var res *gomemcached.MCResponse
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err = mc.SetCas(vb, k, flags, exp, cas, data, context...)
		return err
	})

	if err == nil && (opt&(Persist|Indexable) != 0) {
		err = b.WaitForPersistence(k, res.Cas, data == nil)
	}

	return res.Cas, err
}

// Extended CAS operation. These functions will return the mutation token, i.e vbuuid & guard
func (b *Bucket) CasWithMeta(k string, flags int, exp int, cas uint64, v interface{}, context ...*memcached.ClientContext) (uint64, *MutationToken, error) {
	return b.WriteCasWithMT(k, flags, exp, cas, v, 0, context...)
}

func (b *Bucket) CasWithMetaRaw(k string, flags int, exp int, cas uint64, v interface{}, context ...*memcached.ClientContext) (uint64, *MutationToken, error) {
	return b.WriteCasWithMT(k, flags, exp, cas, v, Raw, context...)
}

func (b *Bucket) WriteCasWithMT(k string, flags, exp int, cas uint64, v interface{},
	opt WriteOptions, context ...*memcached.ClientContext) (newCas uint64, mt *MutationToken, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) {
			ClientOpCallback(fmt.Sprintf("Write(%v)", opt), k, t, err)
		}(time.Now())
	}

	var data []byte
	if opt&Raw == 0 {
		data, err = json.Marshal(v)
		if err != nil {
			return 0, nil, err
		}
	} else if v != nil {
		data = v.([]byte)
	}

	var res *gomemcached.MCResponse
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err = mc.SetCas(vb, k, flags, exp, cas, data, context...)
		return err
	})

	if err != nil {
		return 0, nil, err
	}

	// check for extras
	if len(res.Extras) >= 16 {
		vbuuid := uint64(binary.BigEndian.Uint64(res.Extras[0:8]))
		seqNo := uint64(binary.BigEndian.Uint64(res.Extras[8:16]))
		vb := b.VBHash(k)
		mt = &MutationToken{VBid: uint16(vb), Guard: vbuuid, Value: seqNo}
	}

	if err == nil && (opt&(Persist|Indexable) != 0) {
		err = b.WaitForPersistence(k, res.Cas, data == nil)
	}

	return res.Cas, mt, err
}

// Set a value in this bucket.
// The value will be serialized into a JSON document.
func (b *Bucket) Set(k string, exp int, v interface{}, context ...*memcached.ClientContext) error {
	return b.Write(k, 0, exp, v, 0, context...)
}

// Set a value in this bucket.
func (b *Bucket) SetWithCAS(k string, exp int, v interface{}, context ...*memcached.ClientContext) (uint64, error) {
	return b.WriteWithCAS(k, 0, exp, v, 0, context...)
}

// Set a value in this bucket with with flags
func (b *Bucket) SetWithMeta(k string, flags int, exp int, v interface{}, context ...*memcached.ClientContext) (*MutationToken, error) {
	return b.WriteWithMT(k, flags, exp, v, 0, context...)
}

// SetRaw sets a value in this bucket without JSON encoding it.
func (b *Bucket) SetRaw(k string, exp int, v []byte, context ...*memcached.ClientContext) error {
	return b.Write(k, 0, exp, v, Raw, context...)
}

// Add adds a value to this bucket; like Set except that nothing
// happens if the key exists.  The value will be serialized into a
// JSON document.
func (b *Bucket) Add(k string, exp int, v interface{}, context ...*memcached.ClientContext) (added bool, err error) {
	err = b.Write(k, 0, exp, v, AddOnly, context...)
	if err == ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

// Add adds a value to this bucket; like Set except that nothing
// happens if the key exists. Return the CAS value.
func (b *Bucket) AddWithCAS(k string, exp int, v interface{}, context ...*memcached.ClientContext) (bool, uint64, error) {
	cas, err := b.WriteWithCAS(k, 0, exp, v, AddOnly, context...)
	if err == ErrKeyExists {
		return false, 0, nil
	}
	return (err == nil), cas, err
}

// AddRaw adds a value to this bucket; like SetRaw except that nothing
// happens if the key exists.  The value will be stored as raw bytes.
func (b *Bucket) AddRaw(k string, exp int, v []byte, context ...*memcached.ClientContext) (added bool, err error) {
	err = b.Write(k, 0, exp, v, AddOnly|Raw, context...)
	if err == ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

// Add adds a value to this bucket; like Set except that nothing
// happens if the key exists.  The value will be serialized into a
// JSON document.
func (b *Bucket) AddWithMT(k string, exp int, v interface{}, context ...*memcached.ClientContext) (added bool, mt *MutationToken, err error) {
	mt, err = b.WriteWithMT(k, 0, exp, v, AddOnly, context...)
	if err == ErrKeyExists {
		return false, mt, nil
	}
	return (err == nil), mt, err
}

// AddRaw adds a value to this bucket; like SetRaw except that nothing
// happens if the key exists.  The value will be stored as raw bytes.
func (b *Bucket) AddRawWithMT(k string, exp int, v []byte, context ...*memcached.ClientContext) (added bool, mt *MutationToken, err error) {
	mt, err = b.WriteWithMT(k, 0, exp, v, AddOnly|Raw, context...)
	if err == ErrKeyExists {
		return false, mt, nil
	}
	return (err == nil), mt, err
}

// Append appends raw data to an existing item.
func (b *Bucket) Append(k string, data []byte, context ...*memcached.ClientContext) error {
	return b.Write(k, 0, 0, data, Append|Raw, context...)
}

// Returns collectionUid, manifestUid, error.
func (b *Bucket) GetCollectionCID(scope string, collection string, reqDeadline time.Time) (uint32, uint32, error) {
	var err error
	var response *gomemcached.MCResponse

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetCollectionCID", scope+"."+collection, t, err) }(time.Now())
	}

	var key = "DUMMY" // Contact any server.
	var manifestUid uint32
	var collUid uint32
	err = b.Do2(key, func(mc *memcached.Client, vb uint16) error {
		var err1 error

		mc.SetDeadline(getDeadline(reqDeadline, DefaultTimeout))
		_, err1 = mc.SelectBucket(b.Name)
		if err1 != nil {
			return err1
		}

		response, err1 = mc.CollectionsGetCID(scope, collection)
		if err1 != nil {
			return err1
		}

		manifestUid = binary.BigEndian.Uint32(response.Extras[4:8])
		collUid = binary.BigEndian.Uint32(response.Extras[8:12])

		return nil
	}, false)

	return collUid, manifestUid, err
}

// Get a value straight from Memcached
func (b *Bucket) GetsMC(key string, reqDeadline time.Time, context ...*memcached.ClientContext) (*gomemcached.MCResponse, error) {
	var err error
	var response *gomemcached.MCResponse

	if key == "" {
		return nil, nil
	}

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetsMC", key, t, err) }(time.Now())
	}

	err = b.Do2(key, func(mc *memcached.Client, vb uint16) error {
		var err1 error

		mc.SetDeadline(getDeadline(reqDeadline, DefaultTimeout))
		response, err1 = mc.Get(vb, key, context...)
		if err1 != nil {
			return err1
		}
		return nil
	}, false)
	return response, err
}

// Get a value through the subdoc API
func (b *Bucket) GetsSubDoc(key string, reqDeadline time.Time, subPaths []string, context ...*memcached.ClientContext) (*gomemcached.MCResponse, error) {
	var err error
	var response *gomemcached.MCResponse

	if key == "" {
		return nil, nil
	}

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetsSubDoc", key, t, err) }(time.Now())
	}

	err = b.Do2(key, func(mc *memcached.Client, vb uint16) error {
		var err1 error

		mc.SetDeadline(getDeadline(reqDeadline, DefaultTimeout))
		response, err1 = mc.GetSubdoc(vb, key, subPaths, context...)
		if err1 != nil {
			return err1
		}
		return nil
	}, false)
	return response, err
}

// GetsRaw gets a raw value from this bucket including its CAS
// counter and flags.
func (b *Bucket) GetsRaw(k string, context ...*memcached.ClientContext) (data []byte, flags int,
	cas uint64, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetsRaw", k, t, err) }(time.Now())
	}

	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k, context...)
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

// Gets gets a value from this bucket, including its CAS counter.  The
// value is expected to be a JSON stream and will be deserialized into
// rv.
func (b *Bucket) Gets(k string, rv interface{}, caso *uint64, context ...*memcached.ClientContext) error {
	data, _, cas, err := b.GetsRaw(k, context...)
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
func (b *Bucket) Get(k string, rv interface{}, context ...*memcached.ClientContext) error {
	return b.Gets(k, rv, nil, context...)
}

// GetRaw gets a raw value from this bucket.  No marshaling is performed.
func (b *Bucket) GetRaw(k string, context ...*memcached.ClientContext) ([]byte, error) {
	d, _, _, err := b.GetsRaw(k, context...)
	return d, err
}

// GetAndTouchRaw gets a raw value from this bucket including its CAS
// counter and flags, and updates the expiry on the doc.
func (b *Bucket) GetAndTouchRaw(k string, exp int, context ...*memcached.ClientContext) (data []byte,
	cas uint64, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetsRaw", k, t, err) }(time.Now())
	}

	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.GetAndTouch(vb, k, exp, context...)
		if err != nil {
			return err
		}
		cas = res.Cas
		data = res.Body
		return nil
	})
	return data, cas, err
}

// GetMeta returns the meta values for a key
func (b *Bucket) GetMeta(k string, flags *int, expiry *int, cas *uint64, seqNo *uint64, context ...*memcached.ClientContext) (err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetsMeta", k, t, err) }(time.Now())
	}

	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.GetMeta(vb, k, context...)
		if err != nil {
			return err
		}

		*cas = res.Cas
		if len(res.Extras) >= 8 {
			*flags = int(binary.BigEndian.Uint32(res.Extras[4:]))
		}

		if len(res.Extras) >= 12 {
			*expiry = int(binary.BigEndian.Uint32(res.Extras[8:]))
		}

		if len(res.Extras) >= 20 {
			*seqNo = uint64(binary.BigEndian.Uint64(res.Extras[12:]))
		}

		return nil
	})

	return err
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string, context ...*memcached.ClientContext) error {
	return b.Write(k, 0, 0, nil, Raw, context...)
}

// Incr increments the value at a given key by amt and defaults to def if no value present.
func (b *Bucket) Incr(k string, amt, def uint64, exp int, context ...*memcached.ClientContext) (val uint64, err error) {
	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("Incr", k, t, err) }(time.Now())
	}

	var rv uint64
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Incr(vb, k, amt, def, exp, context...)
		if err != nil {
			return err
		}
		rv = res
		return nil
	})
	return rv, err
}

// Decr decrements the value at a given key by amt and defaults to def if no value present
func (b *Bucket) Decr(k string, amt, def uint64, exp int, context ...*memcached.ClientContext) (val uint64, err error) {
	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("Decr", k, t, err) }(time.Now())
	}

	var rv uint64
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Decr(vb, k, amt, def, exp, context...)
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
	if ClientOpCallback != nil {
		defer func(t time.Time) {
			ClientOpCallback("casNext", k, t, state.Err)
		}(time.Now())
	}

	keepGoing := false
	state.Err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		keepGoing = mc.CASNext(vb, k, exp, state)
		return state.Err
	})
	return keepGoing && state.Err == nil
}

// An UpdateFunc is a callback function to update a document
type UpdateFunc func(current []byte) (updated []byte, err error)

// Return this as the error from an UpdateFunc to cancel the Update
// operation.
const UpdateCancel = memcached.CASQuit

// Update performs a Safe update of a document, avoiding conflicts by
// using CAS.
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

// A WriteUpdateFunc is a callback function to update a document
type WriteUpdateFunc func(current []byte) (updated []byte, opt WriteOptions, err error)

// WriteUpdate performs a Safe update of a document, avoiding
// conflicts by using CAS.  WriteUpdate is like Update, except that
// the callback can return a set of WriteOptions, of which Persist and
// Indexable are recognized: these cause the call to wait until the
// document update has been persisted to disk and/or become available
// to index.
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

// Observe observes the current state of a document.
func (b *Bucket) Observe(k string) (result memcached.ObserveResult, err error) {
	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("Observe", k, t, err) }(time.Now())
	}

	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		result, err = mc.Observe(vb, k)
		return err
	})
	return
}

// Returned from WaitForPersistence (or Write, if the Persistent or Indexable flag is used)
// if the value has been overwritten by another before being persisted.
var ErrOverwritten = errors.New("overwritten")

// Returned from WaitForPersistence (or Write, if the Persistent or Indexable flag is used)
// if the value hasn't been persisted by the timeout interval
var ErrTimeout = errors.New("timeout")

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

var _STRING_MCRESPONSE_POOL = gomemcached.NewStringMCResponsePool(16)

type stringPool struct {
	pool *sync.Pool
	size int
}

func newStringPool(size int) *stringPool {
	rv := &stringPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]string, 0, size)
			},
		},
		size: size,
	}

	return rv
}

func (this *stringPool) Get() []string {
	return this.pool.Get().([]string)
}

func (this *stringPool) Put(s []string) {
	if s == nil || cap(s) < this.size || cap(s) > 2*this.size {
		return
	}

	this.pool.Put(s[0:0])
}

var _STRING_POOL = newStringPool(16)

type vbStringPool struct {
	pool    *sync.Pool
	strPool *stringPool
}

func newVBStringPool(size int, sp *stringPool) *vbStringPool {
	rv := &vbStringPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make(map[uint16][]string, size)
			},
		},
		strPool: sp,
	}

	return rv
}

func (this *vbStringPool) Get() map[uint16][]string {
	return this.pool.Get().(map[uint16][]string)
}

func (this *vbStringPool) Put(s map[uint16][]string) {
	if s == nil {
		return
	}

	for k, v := range s {
		delete(s, k)
		this.strPool.Put(v)
	}

	this.pool.Put(s)
}

var _VB_STRING_POOL = newVBStringPool(16, _STRING_POOL)
