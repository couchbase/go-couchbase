package main

import (
	"flag"
	"fmt"
	"github.com/prataprc/go-couchbase"
	"log"
	"strconv"
	"time"
)

func maybeFatal(err error) {
	if err != nil {
		log.Fatalf("Error:  %v", err)
	}
}

func doOps(b *couchbase.Bucket) {
	fmt.Printf("Doing some ops on %s\n", b.Name)
	start := time.Now()
	total := 2048
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("k%d", i)
		maybeFatal(b.Set(k, 0, []string{"a", "b", "c"}))
		rv := make([]string, 0, 10)
		maybeFatal(b.Get(k, &rv))
		if fmt.Sprintf("%#v", rv) != `[]string{"a", "b", "c"}` {
			log.Fatalf("Expected %#v, got %#v",
				[]string{"a", "b", "c"}, rv)
		}
		maybeFatal(b.Delete(k))
	}
	fmt.Printf("Did %d ops in %s\n",
		total*3, time.Now().Sub(start).String())
}

func exploreBucket(bucket *couchbase.Bucket) {
	vbm := bucket.VBServerMap()
	fmt.Printf("     %v uses %s\n", bucket.Name, vbm.HashAlgorithm)
	for pos, server := range vbm.ServerList {
		vbs := make([]string, 0, 1024)
		for vb, a := range vbm.VBucketMap {
			if a[0] == pos {
				vbs = append(vbs, strconv.Itoa(vb))
			}
		}
		fmt.Printf("        %s: %v\n", server, vbs)
	}

	doOps(bucket)

}

func explorePool(pool couchbase.Pool) {
	for _, n := range pool.Nodes {
		fmt.Printf("     %v\n", n.Hostname)
	}
	fmt.Printf("  Buckets:\n")
	for n := range pool.BucketMap {
		bucket, err := pool.GetBucket(n)
		if err != nil {
			log.Fatalf("Error getting bucket:  %v\n", err)
		}
		exploreBucket(bucket)
	}
}

func main() {
	flag.Parse()
	c, err := couchbase.Connect(flag.Arg(0))
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}
	fmt.Printf("Connected to ver=%s\n", c.Info.ImplementationVersion)
	for _, pn := range c.Info.Pools {
		fmt.Printf("Found pool:  %s -> %s\n", pn.Name, pn.URI)
		p, err := c.GetPool(pn.Name)
		if err != nil {
			log.Fatalf("Can't get pool:  %v", err)
		}
		explorePool(p)
	}
}
