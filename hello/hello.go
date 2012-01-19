package main

import (
	"flag"
	"fmt"
	"github.com/dustin/go-couchbase"
	"log"
)

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
			fmt.Printf("  %v\n", n.Hostname)
		}
	}
}
