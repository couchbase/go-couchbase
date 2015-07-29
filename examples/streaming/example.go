package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"log"
	"time"
)

var serverURL = flag.String("serverURL", "http://localhost:9000",
	"couchbase server URL")
var poolName = flag.String("poolName", "default",
	"pool name")
var bucketName = flag.String("bucketName", "default",
	"bucket name")

func main() {

	flag.Parse()

	client, err := couchbase.Connect(*serverURL)
	if err != nil {
		log.Printf("Connect failed %v", err)
		return
	}

	cbpool, err := client.GetPool("default")
	if err != nil {
		log.Printf("Failed to connect to default pool %v", err)
		return
	}

	var cbbucket *couchbase.Bucket
	cbbucket, err = cbpool.GetBucket(*bucketName)

	if err != nil {
		log.Printf("Failed to connect to bucket %v", err)
		return
	}

	couchbase.SetConnectionPoolParams(256, 16)
	couchbase.SetTcpKeepalive(true, 30)

	go performOp(cbbucket)

	errCh := make(chan error)

	cbbucket.RunBucketUpdater(func(bucket string, err error) {
		log.Printf(" Updated retured err %v", err)
		errCh <- err
	})

	<-errCh

}

func performOp(b *couchbase.Bucket) {

	i := 0
	for {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("value%d", i)
		log.Printf(" setting key %v", key)
		err := b.Set(key, len(value), value)
		if err != nil {
			log.Printf("set failed error %v", err)
			continue
		}
		i++

		<-time.After(1 * time.Second)
	}

}
