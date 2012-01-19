package main

import (
	"flag"
	"fmt"
	"github.com/dustin/go-couchbase"
	"log"
	"strconv"
)

func maybeFatal(err error) {
	if err != nil {
		log.Fatalf("Error:  %v", err)
	}
}

func doOps(b couchbase.Bucket) {
	fmt.Printf("Doing some ops on %s\n", b.Name)
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("k%d", i)
		fmt.Printf("Doing key=%s\n", k)
		maybeFatal(b.Set(k, []string{"a", "b", "c"}))
		rv := make([]string, 0, 10)
		fmt.Printf("Getting %s\n", k)
		maybeFatal(b.Get(k, &rv))
		fmt.Printf("Got back %v\n", rv)
		maybeFatal(b.Delete(k))
	}
}

func exploreBucket(bucket couchbase.Bucket) {
	fmt.Printf("     %v uses %s\n", bucket.Name,
		bucket.VBucketServerMap.HashAlgorithm)
	for pos, server := range bucket.VBucketServerMap.ServerList {
		vbs := make([]string, 0, 1024)
		for vb, a := range bucket.VBucketServerMap.VBucketMap {
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
	for n, _ := range pool.Buckets {
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
