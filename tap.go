package couchbase

import (
	"log"
	"time"

	"github.com/dustin/gomemcached/client"
)

const kInitialRetryInterval = 1 * time.Second
const kMaximumRetryInterval = 30 * time.Second

// A Tap feed. Events from the bucket can be read from the channel 'C'.
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type TapFeed struct {
	C <-chan memcached.TapEvent

	bucket    *Bucket
	args      *memcached.TapArguments
	nodeFeeds []*memcached.TapFeed    // The TAP feeds of the individual nodes
	output    chan memcached.TapEvent // Same as C but writeably-typed
	quit      chan bool
}

// Creates and starts a new Tap feed
func (b *Bucket) StartTapFeed(args *memcached.TapArguments) (*TapFeed, error) {
	if args == nil {
		defaultArgs := memcached.DefaultTapArguments()
		args = &defaultArgs
	}

	feed := &TapFeed{
		bucket: b,
		args:   args,
		output: make(chan memcached.TapEvent, 10),
		quit:   make(chan bool),
	}

	go feed.run()

	feed.C = feed.output
	return feed, nil
}

// Goroutine that runs the feed
func (feed *TapFeed) run() {
	retryInterval := kInitialRetryInterval
	bucketOK := true
	for {
		// Connect to the TAP feed of each server node:
		if bucketOK {
			killSwitch, err := feed.connectToNodes()
			if err == nil {
				// Run until one of the sub-feeds fails:
				select {
				case <-killSwitch:
				case <-feed.quit:
					return
				}
				feed.closeNodeFeeds()
				retryInterval = kInitialRetryInterval
			}
		}

		// On error, try to refresh the bucket in case the list of nodes changed:
		log.Printf("go-couchbase: TAP connection lost; reconnecting to bucket %q in %v",
			feed.bucket.Name, retryInterval)
		err := feed.bucket.refresh()
		bucketOK = err == nil

		select {
		case <-time.After(retryInterval):
		case <-feed.quit:
			return
		}
		if retryInterval *= 2; retryInterval > kMaximumRetryInterval {
			retryInterval = kMaximumRetryInterval
		}
	}
}

func (feed *TapFeed) connectToNodes() (killSwitch chan bool, err error) {
	killSwitch = make(chan bool)
	for _, serverConn := range feed.bucket.getConnPools() {
		var singleFeed *memcached.TapFeed
		singleFeed, err = serverConn.StartTapFeed(feed.args)
		if err != nil {
			feed.closeNodeFeeds()
			return
		}
		feed.nodeFeeds = append(feed.nodeFeeds, singleFeed)
		go feed.forwardTapEvents(singleFeed, killSwitch)
	}
	return
}

// Goroutine that forwards Tap events from a single node's feed to the aggregate feed.
func (feed *TapFeed) forwardTapEvents(singleFeed *memcached.TapFeed, killSwitch chan bool) {
	for {
		select {
		case event, ok := <-singleFeed.C:
			if !ok {
				killSwitch <- true
				return
			}
			feed.output <- event
		case <-feed.quit:
			return
		}
	}
}

func (feed *TapFeed) closeNodeFeeds() {
	for _, f := range feed.nodeFeeds {
		f.Close()
	}
	feed.nodeFeeds = nil
}

// Closes a Tap feed.
func (feed *TapFeed) Close() error {
	select {
	case <-feed.quit:
		return nil
	default:
	}

	feed.closeNodeFeeds()
	close(feed.quit)
	close(feed.output)
	return nil
}
