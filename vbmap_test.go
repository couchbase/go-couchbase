package couchbase

import (
	"testing"
)

func testBucket() Bucket {
	b := Bucket{}
	b.VBucketServerMap.VBucketMap = make([][]int, 1024)
	return b
}

func TestVBHash(t *testing.T) {
	b := testBucket()
	b.VBHash("somekey")
}
