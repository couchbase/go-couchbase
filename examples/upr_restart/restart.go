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
	b, err := getTestConnection("default")
	if err != nil {
		panic(err)
	}
	// start upr feed
	feed, err := couchbase.StartUprFeed(b, "index" /*name*/, nil)
	if err != nil {
		panic(err)
	}

	// add mutations to the bucket
	var mutationCount = 5
	addKVset(bucket, mutationCount)

	vbseqNo := receiveMutations(feed, mutationCount)
	for vbno, seqno := range vbseqNo {
		stream := feed.GetStream(uint16(vbno))
		if stream.Startseq != seqno {
			panic(fmt.Errorf(
				"For vbucket %v, stream seqno is %v, received is %v",
				vbno, stream.Startseq, seqno))
		}
	}

	streams := make(map[uint16]*couchbase.UprStream, 0)
	for vb := range vbseqNo {
		stream := feed.GetStream(uint16(vb))
		streams[uint16(vb)] = stream
		stream.Vuuid = stream.Flog[0][0]
	}
	feed.Close()

	log.Println("Restarting ....")
	feed, err = couchbase.StartUprFeed(b, "index" /*name*/, streams)
	if err != nil {
		panic(err)
	}

	newkey, newvalue := "newkey", "new mutation"
	if err = bucket.Set(newkey, 0, newvalue); err != nil {
		panic(err)
	}

	<-time.After(2 * time.Second)

	e := <-feed.C
	exptSeq := vbseqNo[e.Vbucket] + 1

	if e.Seqno != exptSeq {
		err := fmt.Errorf("Expected seqno %v, received %v", exptSeq+1, e.Seqno)
		panic(err)
	}
	if string(e.Key) != newkey {
		err := fmt.Errorf("Expected key %v received %v", newkey, string(e.Key))
		panic(err)
	}
	if string(e.Value) != fmt.Sprintf("%q", newvalue) {
		err := fmt.Errorf("Expected value %v received %v", newvalue, string(e.Value))
		panic(err)
	}
	feed.Close()
}

func addKVset(b *couchbase.Bucket, count int) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key%v", i)
		value := fmt.Sprintf("Hello world%v", i)
		if err := b.Set(key, 0, value); err != nil {
			panic(err)
		}
	}
}

func receiveMutations(feed *couchbase.UprFeed, mutationCount int) []uint64 {
	var vbseqNo = make([]uint64, vbcount)
	var mutations = 0
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
				"sequence number for vbucket %v, is %v, expected %v",
				e.Vbucket, e.Seqno, vbseqNo[e.Vbucket]+1)
		}
		mutations += 1
	}
	count := 0
	for _, seqNo := range vbseqNo {
		count += int(seqNo)
	}
	if count != mutationCount {
		panic(fmt.Errorf("Expected %v mutations, got %v", mutationCount, count))
	}
	return vbseqNo
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(TESTURL)
	if err != nil {
		fmt.Println("Make sure that couchbase is at", TESTURL)
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketname)
	return bucket, err
}
