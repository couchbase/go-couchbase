package couchbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
)

func (b *Bucket) do(k string, f func(mc *memcachedClient, vb uint16) error) error {
	vb := b.VBHash(k)
	for {
		masterId := b.VBucketServerMap.VBucketMap[vb][0]
		if b.connections[masterId] == nil {
			b.connections[masterId] = connect("tcp", b.VBucketServerMap.ServerList[masterId])
		}
		err := f(b.connections[masterId], uint16(vb))
		switch err.(type) {
		default:
			return err
		case mcResponse:
			if err.(mcResponse).Status == mcNOT_MY_VBUCKET {
				b.pool.refreshBuckets()
			} else {
				return err
			}
		}
	}
	panic("Unreachable.")
}

// Set a value in this bucket.
// The value will be serialized into a JSON document.
func (b *Bucket) Set(k string, v interface{}) error {
	return b.do(k, func(mc *memcachedClient, vb uint16) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		res := mc.Set(vb, k, 0, 0, data)
		if res.Status != mcSUCCESS {
			return res
		}
		return nil
	})
}

// Get a value from this bucket.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Get(k string, rv interface{}) error {
	return b.do(k, func(mc *memcachedClient, vb uint16) error {
		res := mc.Get(vb, k)
		if res.Status != mcSUCCESS {
			return res
		}
		return json.Unmarshal(res.Body, rv)
	})
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string) error {
	return b.do(k, func(mc *memcachedClient, vb uint16) error {
		res := mc.Del(vb, k)
		if res.Status != mcSUCCESS {
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
