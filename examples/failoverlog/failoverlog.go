package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/couchbaselabs/go-couchbase"
)

const clusterAddr = "http://localhost:9000"

var options struct {
	clusterAddr string
	repeat      int
	repeatWait  int
}

func argParse() {
	flag.IntVar(&options.repeat, "repeat", 1,
		"number of time to repeat fetching failover log")
	flag.IntVar(&options.repeatWait, "repeatWait", 1000,
		"time in mS, to wait before next repeat")

	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		options.clusterAddr = clusterAddr
	} else {
		options.clusterAddr = args[0]
	}
}

func main() {
	argParse()
	// get a bucket and mc.Client connection
	bucket, err := getTestConnection("default")
	if err != nil {
		panic(err)
	}

	// Get failover log for a vbucket
	for options.repeat > 0 {
		flogs, err := bucket.GetFailoverLogs([]uint16{0, 1, 2, 3, 4, 5, 6, 7})
		if err != nil {
			panic(err)
		}
		for vbno, flog := range flogs {
			log.Printf("Failover logs for vbucket %v: %v", vbno, flog)
		}
		options.repeat--
		if options.repeat > 0 {
			fmt.Println()
			time.Sleep(time.Duration(options.repeatWait) * time.Millisecond)
		}
	}
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(options.clusterAddr)
	if err != nil {
		log.Println("Make sure that couchbase is at", options.clusterAddr)
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketname)
	return bucket, err
}
