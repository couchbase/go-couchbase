package couchbase

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goutils/logging"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
	"unsafe"
)

// Bucket auto-updater gets the latest version of the bucket config from
// the server. If the configuration has changed then updated the local
// bucket information. If the bucket has been deleted then notify anyone
// who is holding a reference to this bucket

const MAX_RETRY_COUNT = 5
const DISCONNECT_PERIOD = 120 * time.Second

type NotifyFn func(bucket string, err error)
type StreamingFn func(bucket *Bucket)

// Use TCP keepalive to detect half close sockets
var updaterTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
}

var updaterHTTPClient = &http.Client{Transport: updaterTransport}

func doHTTPRequestForUpdate(req *http.Request) (*http.Response, error) {

	var err error
	var res *http.Response

	for i := 0; i < HTTP_MAX_RETRY; i++ {
		res, err = updaterHTTPClient.Do(req)
		if err != nil && isHttpConnError(err) {
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}

	return res, err
}

func (b *Bucket) RunBucketUpdater(notify NotifyFn) {
	b.RunBucketUpdater2(nil, notify)
}

func (b *Bucket) RunBucketUpdater2(streamingFn StreamingFn, notify NotifyFn) {

	b.Lock()
	if b.updater != nil {
		b.updater.Close()
		b.updater = nil
	}
	b.Unlock()
	go func() {
		err := b.UpdateBucket()
		if err != nil {
			if notify != nil {
				name := b.GetName()
				notify(name, err)

				// MB-49772 get rid of the deleted bucket
				p := b.pool
				b.Close()
				p.Lock()
				p.BucketMap[name] = nil
				delete(p.BucketMap, name)
				p.Unlock()
			}
			logging.Errorf(" Bucket Updater exited with err %v", err)
		}
	}()
}

func (b *Bucket) replaceConnPools2(with []*connectionPool, bucketLocked bool) {
	if !bucketLocked {
		b.Lock()
		defer b.Unlock()
	}
	old := b.connPools
	b.connPools = unsafe.Pointer(&with)
	if old != nil {
		for _, pool := range *(*[]*connectionPool)(old) {
			if pool != nil && pool.inUse == false {
				pool.Close()
			}
		}
	}
	return
}

func (b *Bucket) UpdateBucket() error {
	return b.UpdateBucket2(nil)
}

func (b *Bucket) UpdateBucket2(streamingFn StreamingFn) error {
	var failures int
	var returnErr error
	var poolServices PoolServices
	var updater io.ReadCloser

	defer func() {
		b.Lock()
		if b.updater == updater {
			b.updater = nil
		}
		b.Unlock()
	}()

	for {

		if failures == MAX_RETRY_COUNT {
			logging.Errorf(" Maximum failures reached. Exiting loop...")
			return fmt.Errorf("Max failures reached. Last Error %v", returnErr)
		}

		nodes := b.Nodes()
		if len(nodes) < 1 {
			return fmt.Errorf("No healthy nodes found")
		}

		streamUrl := fmt.Sprintf("%s/pools/default/bucketsStreaming/%s", b.pool.client.BaseURL, uriAdj(b.GetName()))
		logging.Infof(" Trying with %s", streamUrl)
		req, err := http.NewRequest("GET", streamUrl, nil)
		if err != nil {
			return err
		}

		// Lock here to avoid having pool closed under us.
		b.RLock()
		err = maybeAddAuth(req, b.pool.client.ah)
		b.RUnlock()
		if err != nil {
			return err
		}

		res, err := doHTTPRequestForUpdate(req)
		if err != nil {
			return err
		}

		if res.StatusCode != 200 {
			bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
			logging.Errorf("Failed to connect to host, unexpected status code: %v. Body %s", res.StatusCode, bod)
			res.Body.Close()
			returnErr = fmt.Errorf("Failed to connect to host. Status %v Body %s", res.StatusCode, bod)
			failures++
			continue
		}
		b.Lock()
		if b.updater == updater {
			b.updater = res.Body
			updater = b.updater
		} else {
			// another updater is running and we should exit cleanly
			b.Unlock()
			res.Body.Close()
			logging.Debugf("Bucket updater: New updater found for bucket: %v", b.GetName())
			return nil
		}
		b.Unlock()

		dec := json.NewDecoder(res.Body)

		tmpb := &Bucket{}
		for {

			err := dec.Decode(&tmpb)
			if err != nil {
				returnErr = err
				res.Body.Close()
				// if this was closed under us it means a new updater is starting so exit cleanly
				if strings.Contains(err.Error(), "use of closed network connection") {
					logging.Debugf("Bucket updater: Notified of new updater for bucket: %v", b.GetName())
					return nil
				}
				break
			}

			// if we got here, reset failure count
			failures = 0

			if b.pool.client.tlsConfig != nil {
				poolServices, err = b.pool.client.GetPoolServices("default")
				if err != nil {
					returnErr = err
					res.Body.Close()
					break
				}
			}

			b.Lock()

			// mark all the old connection pools for deletion
			pools := b.getConnPools(true /* already locked */)
			for _, pool := range pools {
				if pool != nil {
					pool.inUse = false
				}
			}

			newcps := make([]*connectionPool, len(tmpb.VBSMJson.ServerList))
			for i := range newcps {
				// get the old connection pool and check if it is still valid
				pool := b.getConnPoolByHost(tmpb.VBSMJson.ServerList[i], true /* bucket already locked */)
				if pool != nil && pool.inUse == false && pool.tlsConfig == b.pool.client.tlsConfig {
					// if the hostname and index is unchanged then reuse this pool
					newcps[i] = pool
					pool.inUse = true
					continue
				}
				// else create a new pool
				var encrypted bool
				hostport := tmpb.VBSMJson.ServerList[i]
				if b.pool.client.tlsConfig != nil {
					hostport, encrypted, err = MapKVtoSSLExt(hostport, &poolServices, b.pool.client.disableNonSSLPorts)
					if err != nil {
						b.Unlock()
						return err
					}
				}
				if b.ah != nil {
					newcps[i] = newConnectionPool(hostport,
						b.ah, false, PoolSize, PoolOverflow, b.pool.client.tlsConfig, b.Name, encrypted)

				} else {
					newcps[i] = newConnectionPool(hostport,
						b.authHandler(true /* bucket already locked */),
						false, PoolSize, PoolOverflow, b.pool.client.tlsConfig, b.Name, encrypted)
				}
			}

			b.replaceConnPools2(newcps, true /* bucket already locked */)

			tmpb.ah = b.ah
			b.vBucketServerMap = unsafe.Pointer(&tmpb.VBSMJson)
			b.nodeList = unsafe.Pointer(&tmpb.NodesJSON)
			b.Unlock()

			if streamingFn != nil {
				streamingFn(tmpb)
			}
			logging.Infof("Got new configuration for bucket %s", b.GetName())

		}
		// we are here because of an error
		failures++
		continue

	}
	return nil
}
