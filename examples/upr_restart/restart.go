package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

const clusterAddr = "http://localhost:9000"

var options struct {
	clusterAddr string
	bucket      string
	maxVb       int
	tick        int
	debug       bool
	cpuprofile  string
	memprofile  string
}

func argParse() {
	flag.StringVar(&options.bucket, "bucket", "default",
		"bucket to connect to (defaults to username)")
	flag.IntVar(&options.maxVb, "maxvb", 1024,
		"number configured vbuckets")
	flag.IntVar(&options.tick, "tick", 1000,
		"timer tick in mS to log information")
	flag.BoolVar(&options.debug, "debug", false,
		"number configured vbuckets")
	flag.StringVar(&options.cpuprofile, "cpuprofile", "",
		"write cpu profile to file")
	flag.StringVar(&options.memprofile, "memprofile", "",
		"write memory profile to this file")

	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		options.clusterAddr = clusterAddr
	} else {
		options.clusterAddr = args[0]
	}
}

// Flush the bucket before trying this program
func main() {
	argParse()

	// get a bucket and mc.Client connection
	bucket, err := getTestConnection(options.bucket)
	if err != nil {
		log.Fatal(err)
	}

	// start upr feed
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feed, err := bucket.StartUprFeed(name, 0)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < options.maxVb; i++ {
		err := feed.UprRequestStream(
			uint16(i) /*vbno*/, uint16(0) /*opaque*/, 0 /*flag*/, 0, /*vbuuid*/
			0 /*seqStart*/, 0xFFFFFFFFFFFFFFFF /*seqEnd*/, 0 /*snaps*/, 0)
		if err != nil {
			fmt.Printf("%s", err.Error())
		}
	}

	vbseqNo := receiveMutations(feed, 20000)

	vbList := make([]uint16, 0)
	for i := 0; i < options.maxVb; i++ {
		vbList = append(vbList, uint16(i))
	}
	failoverlogMap, err := bucket.GetFailoverLogs(vbList)
	if err != nil {
		log.Printf(" error in failover log request %s", err.Error())

	}

	// get a bucket and mc.Client connection
	bucket1, err := getTestConnection(options.bucket)
	if err != nil {
		panic(err)
	}

	// add mutations to the bucket
	var mutationCount = 5000
	addKVset(bucket1, mutationCount)

	log.Println("Restarting ....")
	feed, err = bucket.StartUprFeed(name, 0)
	if err != nil {
		panic(err)
	}

	for i := 0; i < options.maxVb; i++ {
		log.Printf("Vbucket %d High sequence number %d, Snapshot end sequence %d", i, vbseqNo[i][0], vbseqNo[i][1])
		failoverLog := failoverlogMap[uint16(i)]
		err := feed.UprRequestStream(
			uint16(i) /*vbno*/, uint16(0) /*opaque*/, 0, /*flag*/
			failoverLog[0][0],                              /*vbuuid*/
			vbseqNo[i][0] /*seqStart*/, 0xFFFFFFFFFFFFFFFF, /*seqEnd*/
			0 /*snaps*/, vbseqNo[i][1])
		if err != nil {
			log.Fatal(err)
		}
	}

	var e *memcached.UprEvent
	var mutations int
loop:
	for {
		select {
		case f := <-feed.C:
			if f.Opcode == gomemcached.UPR_MUTATION {
				vbseqNo[f.VBucket][0] = f.Seqno
				e = f
				mutations += 1
			}
		case <-time.After(time.Second):
			break loop
		}
	}

	log.Printf(" got %d mutations", mutations)

	exptSeq := vbseqNo[e.VBucket][0] + 1

	if e.Seqno != exptSeq {
		fmt.Printf("Expected seqno %v, received %v", exptSeq+1, e.Seqno)
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

func receiveMutations(feed *couchbase.UprFeed, breakAfter int) [][2]uint64 {
	var vbseqNo = make([][2]uint64, options.maxVb)
	var mutations = 0
	var ssMarkers = 0
loop:
	for {
		select {
		case e := <-feed.C:
			if e.Opcode == gomemcached.UPR_MUTATION {
				vbseqNo[e.VBucket][0] = e.Seqno
				mutations += 1
			}
			if e.Opcode == gomemcached.UPR_SNAPSHOT {
				vbseqNo[e.VBucket][1] = e.SnapendSeq
				ssMarkers += 1
			}
			if mutations == breakAfter {
				break loop
			}

		case <-time.After(time.Second):
			break loop
		}
	}
	log.Printf(" Mutation count %d, Snapshot markers %d", mutations, ssMarkers)
	return vbseqNo
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(options.clusterAddr)
	if err != nil {
		fmt.Println("Make sure that couchbase is at", options.clusterAddr)
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketname)
	return bucket, err
}
