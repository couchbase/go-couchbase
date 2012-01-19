package couchbase

import (
	"encoding/json"
)

func (b *Bucket) getVBInfo(k string) (*memcachedClient, uint16) {
	vb := b.VBHash(k)
	masterId := b.VBucketServerMap.VBucketMap[vb][0]
	if b.connections[masterId] == nil {
		b.connections[masterId] = connect("tcp", b.VBucketServerMap.ServerList[masterId])
	}
	return b.connections[masterId], uint16(vb)
}

// Set a value in this bucket.
// The value will be serialized into a JSON document.
func (b *Bucket) Set(k string, v interface{}) error {
	mc, vb := b.getVBInfo(k)
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	res := mc.Set(vb, k, 0, 0, data)
	if res.Status != mcSUCCESS {
		return res
	}
	return nil
}

// Get a value from this bucket.
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) Get(k string, rv interface{}) error {
	mc, vb := b.getVBInfo(k)
	res := mc.Get(vb, k)
	if res.Status != mcSUCCESS {
		return res
	}
	return json.Unmarshal(res.Body, rv)
}

// Delete a key from this bucket.
func (b *Bucket) Delete(k string) error {
	mc, vb := b.getVBInfo(k)
	res := mc.Del(vb, k)
	if res.Status != mcSUCCESS {
		return res
	}
	return nil
}
