package main

import (
	"flag"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"math/rand"
	"time"
)

var poolName = flag.String("pool", "default", "Pool to connect to")
var bucketName = flag.String("bucket", "default", "Bucket to connect to")

const myfmt = "2006-02-01-15:04:05.000000000"

var names = []string{
	"Jan Lehnardt",
	"John Christopher Anderson",
	"Noah Slater",
	"Filipe David Borba Manana",
	"Adam Kocoloski",
	"Paul Joseph Davis",
	"Christopher Lenz",
	"Damien F. Katz",
	"Robert Newson",
	"Benoit Chesneau",
	"Jason David Davies",
	"Mark Hammond",
	"Randall Leeds",
	"Bin Cui",
	"Benjamin Young",
	"Dustin Sallings",
	"Steve Yen",
	"Joe Schaefer",
}

var actions = []string{
	"submitted", "aborted", "approved", "declined",
}

var projects = []string{
	"ep-engine", "couchdb", "ns_server", "moxi", "libcouchbase",
}

type Record struct {
	Author   string `json:"author"`
	Reviewer string `json:"reviewer"`
	Action   string `json:"action"`
	Project  string `json:"project"`
	Score    int    `json:"score"`
}

func harass(b couchbase.Bucket) {
	fmt.Printf("Doing stuff\n")

	for {
		r := Record{
			Author:   names[rand.Intn(len(names))],
			Reviewer: names[rand.Intn(len(names))],
			Action:   actions[rand.Intn(len(actions))],
			Project:  projects[rand.Intn(len(projects))],
			Score:    rand.Intn(4) - 2,
		}

		k := time.Now().Format(myfmt)

		fmt.Printf("%s:  %#v\n", k, r)
		if err := b.Set(k, r); err != nil {
			log.Fatalf("Oops, failed a store of %s:  %v", k, err)
		}
	}
}

func main() {
	flag.Parse()
	c, err := couchbase.Connect(flag.Arg(0))
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool(*poolName)
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket(*bucketName)
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

	harass(bucket)
}
