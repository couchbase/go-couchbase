package main

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"time"
)

var vbcount = 8

const TESTURL = "http://localhost:9000"

// Flush the bucket before trying this program
func main() {
	// get a bucket and mc.Client connection
	bucket, err := getTestConnection("default")
	if err != nil {
		panic(err)
	}

	// start upr feed
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feed, err := couchbase.StartUprFeed(bucket, name, nil)
	if err != nil {
		panic(err)
	}

	// add mutations to the bucket.
	var vbseqNo = make([]uint64, vbcount)
	var mutationCount = 1000
	var mutations = 0
	addKVset(bucket, mutationCount)

	// observe the mutations from the channel.
	var e couchbase.UprEvent
loop:
	for {
		select {
		case e = <-feed.C:
		case <-time.After(time.Second):
			break loop
		}
		if vbseqNo[e.Vbucket] == 0 {
			vbseqNo[e.Vbucket] = e.Seqno
		} else if vbseqNo[e.Vbucket]+1 == e.Seqno {
			vbseqNo[e.Vbucket] = e.Seqno
		} else {
			log.Printf(
				"sequence number for vbucket %v, is %v, expected %v (%v:%v)\n",
				e.Vbucket, e.Seqno, vbseqNo[e.Vbucket]+1, string(e.Key),
				string(e.Value))
			continue
		}
		mutations += 1
	}
	feed.Close()

	count := 0
	for _, seqNo := range vbseqNo {
		count += int(seqNo)
	}
	log.Println("vb sequence:", vbseqNo)
	if count == mutationCount {
		log.Printf(
			"Created %v mutations and observed %v mutations",
			mutationCount, count)
	} else {
		panic(fmt.Errorf("Expected %v mutations got %v", mutationCount, count))
	}
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(TESTURL)
	if err != nil {
		log.Println("Make sure that couchbase is at", TESTURL)
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
