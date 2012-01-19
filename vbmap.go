package couchbase

import (
	"hash/crc32"
)

func (b *Bucket) VBHash(key string) uint32 {
	// C Code
	// uint32_t digest = hash_crc32(key, nkey);
	// vb->mask = vb->num_vbuckets - 1
	// return digest & vb->mask;

	digest := crc32.ChecksumIEEE([]byte(key))
	return digest & uint32(len(b.connections)-1)
}
