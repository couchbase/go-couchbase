package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/couchbaselabs/go-couchbase"
)

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func main() {

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

	bucketInfo, err := couchbase.GetBucketList(u.String())
	fmt.Printf("List of buckets and password %v", bucketInfo)

}
