package main

import (
	"flag"
	"fmt"
	mc "github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"net/url"
	"os"
	"time"
)

var vbcount = 64

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

// Flush the bucket before trying this program
func main() {

	bname := flag.String("bucket", "",
		"bucket to connect to (defaults to username)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"%v [flags] http://user:pass@host:8091/\n\nFlags:\n",
			os.Args[0])
		flag.PrintDefaults()
		os.Exit(64)
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
	}

	u, err := url.Parse(flag.Arg(0))
	mf(err, "parse")

	if *bname == "" && u.User != nil {
		*bname = u.User.Username()
	}

	c, err := couchbase.Connect(u.String())
	mf(err, "connect - "+u.String())

	p, err := c.GetPool("default")
	mf(err, "pool")

	bucket, err := p.GetBucket(*bname)
	mf(err, "bucket")

	// get failover logs for a few vbuckets
	vbList := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	failoverlogMap, err := bucket.GetFailoverLogs(vbList)
	if err != nil {
		mf(err, "failoverlog")
	}

	for vb, flog := range failoverlogMap {
		log.Printf("Failover log for vbucket %d is %v", vb, flog)
	}

	// start upr feed
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feed, err := bucket.StartUprFeed(name, 0)
	if err != nil {
		panic(err)
	}

	// get the vbucket map for this bucket
	vbm := bucket.VBServerMap()
	log.Println(vbm)

	// request stream for a few vbuckets
	for i := 0; i < len(vbList); i++ {
		if err := feed.UprRequestStream(uint16(i), 0, 0, 0, 0xFFFFFFFFFFFFFFFF, 0, 0); err != nil {
			fmt.Printf("%s", err.Error())
		}
	}

	go addKVset(bucket, 10000)
	var e *mc.UprEvent
	keys := make(map[string]string)
	counts := make(map[mc.UprOpcode]int)

loop:
	for {
		select {
		case e = <-feed.C:
		case <-time.After(time.Second):
			break loop
		}
		if e.Opcode == mc.UprMutation {
			keys[string(e.Key)] = string(e.Value)
		}
		if _, ok := counts[e.Opcode]; !ok {
			counts[e.Opcode] = 0
		}
		counts[e.Opcode]++
	}
	fmt.Println(len(keys))
	feed.Close()
	log.Printf("counts %v", counts)

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
