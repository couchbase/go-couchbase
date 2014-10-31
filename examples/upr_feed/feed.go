package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime/pprof"
	"time"

	mcd "github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached/client"
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

func usage() {
	fmt.Fprintf(os.Stderr,
		"%v [flags] http://user:pass@host:8091/\n\nFlags:\n",
		os.Args[0])
	flag.PrintDefaults()
	os.Exit(64)
}

// Flush the bucket before trying this program
func main() {
	argParse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if options.cpuprofile != "" {
		f, err := os.Create(options.cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if options.memprofile != "" {
		f, err := os.Create(options.memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.WriteHeapProfile(f)
		defer f.Close()
	}

	bucket := getBucket()

	//addKVset(bucket, 1000)
	//return

	// start upr feed
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	feed, err := bucket.StartUprFeed(name, 0)
	if err != nil {
		log.Print(" Failed to start stream ", err)
		return
	}

	// request stream for all vbuckets
	for i := 0; i < options.maxVb; i++ {
		err := feed.UprRequestStream(
			uint16(i) /*vbno*/, uint16(0) /*opaque*/, 0 /*flag*/, 0, /*vbuuid*/
			0 /*seqStart*/, 0xFFFFFFFFFFFFFFFF /*seqEnd*/, 0 /*snaps*/, 0)
		if err != nil {
			fmt.Printf("%s", err.Error())
		}
	}

	// observe the mutations from the channel.
	tick := time.Tick(time.Duration(options.tick) * time.Millisecond)
	var mutations = 0

	for {
		select {
		case e := <-feed.C:
			if e.Opcode == mcd.UPR_MUTATION {
				mutations += 1
			}
			handleEvent(e)

		case <-tick:
			log.Printf("Mutation count %d", mutations)
		}
	}
	feed.Close()
}

func handleEvent(e *mc.UprEvent) {
	if e.Opcode == mcd.UPR_MUTATION && options.debug {
		log.Printf("got mutation %s", e.Value)
	}
	if e.Opcode == mcd.UPR_STREAMEND {
		log.Printf("received Stream end for vbucket %d", e.VBucket)
	}
}

func addKVset(b *couchbase.Bucket, count int) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key%v", i+1000000)
		val_len := rand.Intn(10*1024) + rand.Intn(10*1024)
		value := fmt.Sprintf("This is a test key %d", val_len)
		err := b.Set(key, 0, value)
		if err != nil {
			panic(err)
		}

		if i%100000 == 0 {
			fmt.Printf("\n Added %d keys", i)
		}
	}
}

func getBucket() *couchbase.Bucket {
	u, err := url.Parse(options.clusterAddr)
	mf(err, "parse")

	if options.bucket == "" && u.User != nil {
		options.bucket = u.User.Username()
	}

	c, err := couchbase.Connect(u.String())
	mf(err, "connect - "+u.String())

	p, err := c.GetPool("default")
	mf(err, "pool")

	bucket, err := p.GetBucket(options.bucket)
	mf(err, "bucket")
	return bucket
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

// after receving 1000 mutations close some streams
//if callOnce == false {
//    for i := 0; i < options.maxVb; i = i + 4 {
//        log.Printf("closing stream for vbucket %d", i)
//        if err := feed.UprCloseStream(uint16(i), uint16(0)); err != nil {
//            log.Printf("received error while closing stream %d", i)
//        }
//    }
//    callOnce = true
//}
