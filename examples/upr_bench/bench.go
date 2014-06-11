package main

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"time"
)

var vbcount = 8

const testURL = "http://localhost:9000"

// Flush the bucket before trying this program
func main() {
	// get a bucket and mc.Client connection
	bucket, err := getTestConnection("default")
	if err != nil {
		panic(err)
	}

	// start upr feed
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feed, err := bucket.StartUprFeed(name, 0)
	if err != nil {
		panic(err)
	}

	// add mutations to the bucket.
	var mutationCount = int64(100000)
	go addKVset(bucket, int(mutationCount))

	// request stream for a few vbuckets
	for i := 0; i < 64; i++ {
		if err := feed.UprRequestStream(uint16(i), 0, 0, 0, 0xFFFFFFFFFFFFFFFF, 0, 0); err != nil {
			fmt.Printf("%s", err.Error())
		}
	}

	// observe the mutations from the channel.
	mutations := int64(0)
	start := time.Now().UnixNano()
	end := start
	for {
		<-feed.C
		mutations++
		if mutationCount == mutations {
			break
		}
		if mutations%(mutationCount/5) == 0 {
			end = time.Now().UnixNano()
			log.Printf("Recieved %v mutations %v time lapse per mutation in nano seconds\n",
				mutations, (end-start)/mutations)
		}
	}

	log.Printf(" Total time for %d mutations %d ms", mutations, (end-start)/1000000)

	feed.Close()
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(testURL)
	if err != nil {
		log.Println("Make sure that couchbase is at", testURL)
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketname)
	return bucket, err
}

func addKVset(b *couchbase.Bucket, count int) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key%v", i)
		value := fmt.Sprintf("Hello world%v", i)
		err := b.Set(key, 0, value)
		if err != nil {
			panic(err)
		}
	}
}
