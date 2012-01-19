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
	maybeFatal(b.Set("key1", []string{"a", "b", "c"}))
	rv := make([]string, 0, 10)
	maybeFatal(b.Get("key1", &rv))
	log.Printf("Got back %v", rv)
	maybeFatal(b.Delete("key1"))
}

func main() {
	flag.Parse()
	c, err := couchbase.Connect(flag.Arg(0))
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}
	for _, pn := range c.Info.Pools {
		fmt.Printf("Found pool:  %s -> %s\n", pn.Name, pn.URI)
		p, err := c.GetPool(pn.Name)
		if err != nil {
			log.Fatalf("Can't get pool:  %v", err)
		}
		for _, n := range p.Nodes {
			fmt.Printf("     %v\n", n.Hostname)
		}
		fmt.Printf("  Buckets:\n")
		for n, _ := range p.Buckets {
			bucket, err := p.GetBucket(n)
			if err != nil {
				log.Fatalf("Error getting bucket:  %v\n", err)
			}
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
	}
}
