package couchbase

import (
	"sync"

	"github.com/dustin/gomemcached/client"
)

// A Tap feed. Events from the bucket can be read from the channel 'C'.
// Remember to call Close() on it when you're done, unless its channel has closed itself already.
type TapFeed struct {
	C       <-chan memcached.TapEvent
	output  chan memcached.TapEvent
	feeds   []*memcached.TapFeed
	waiters sync.WaitGroup
}

// Creates and starts a new Tap feed
func (b *Bucket) StartTapFeed(args *memcached.TapArguments) (*TapFeed, error) {
	if args == nil {
		defaultArgs := memcached.DefaultTapArguments()
		args = &defaultArgs
	}

	feed := &TapFeed{output: make(chan memcached.TapEvent, 10)}
	for _, serverConn := range b.connections {
		singleFeed, err := serverConn.StartTapFeed(args)
		if err != nil {
			feed.Close()
			return nil, err
		}
		feed.feeds = append(feed.feeds, singleFeed)
		feed.waiters.Add(1)
		go feed.forwardTapEvents(singleFeed.C)
	}
	go feed.closeWhenDone()

	feed.C = feed.output
	return feed, nil

}

// Goroutine that forwards Tap events from a single node's feed to the aggregate feed.
func (feed *TapFeed) forwardTapEvents(singleFeed <-chan memcached.TapEvent) {
	defer feed.waiters.Done()
	for {
		if event, ok := <-singleFeed; ok {
			feed.output <- event
		} else {
			break
		}
	}
}

// Goroutine that closes the bucket's tap feed channel when all input feeds have closed
func (feed *TapFeed) closeWhenDone() {
	feed.waiters.Wait()
	close(feed.output)
}

// Closes a Tap feed.
func (feed *TapFeed) Close() {
	for _, f := range feed.feeds {
		f.Close()
	}
	feed.feeds = nil
	feed.C = nil
}
