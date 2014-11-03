package couchbase

import (
	"errors"
	"time"

	"github.com/couchbase/gomemcached/client"
)

// Error raised when a connection can't be retrieved from a pool.
var TimeoutError = errors.New("timeout waiting to build connection")
var errClosedPool = errors.New("the connection pool is closed")
var errNoPool = errors.New("no connection pool")

// Default timeout for retrieving a connection from the pool.
var ConnPoolTimeout = time.Hour * 24 * 30

// ConnPoolAvailWaitTime is the amount of time to wait for an existing
// connection from the pool before considering the creation of a new
// one.
var ConnPoolAvailWaitTime = time.Millisecond

type connectionPool struct {
	host        string
	mkConn      func(host string, ah AuthHandler) (*memcached.Client, error)
	auth        AuthHandler
	connections chan *memcached.Client
	createsem   chan bool
}

func newConnectionPool(host string, ah AuthHandler, poolSize, poolOverflow int) *connectionPool {
	return &connectionPool{
		host:        host,
		connections: make(chan *memcached.Client, poolSize),
		createsem:   make(chan bool, poolSize+poolOverflow),
		mkConn:      defaultMkConn,
		auth:        ah,
	}
}

// ConnPoolTimeout is notified whenever connections are acquired from a pool.
var ConnPoolCallback func(host string, source string, start time.Time, err error)

func defaultMkConn(host string, ah AuthHandler) (*memcached.Client, error) {
	conn, err := memcached.Connect("tcp", host)
	if err != nil {
		return nil, err
	}
	name, pass := ah.GetCredentials()
	if name != "default" {
		_, err = conn.Auth(name, pass)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (cp *connectionPool) Close() (err error) {
	defer func() { err, _ = recover().(error) }()
	close(cp.connections)
	for c := range cp.connections {
		c.Close()
	}
	return
}

func (cp *connectionPool) GetWithTimeout(d time.Duration) (rv *memcached.Client, err error) {
	if cp == nil {
		return nil, errNoPool
	}

	path := ""

	if ConnPoolCallback != nil {
		defer func(path *string, start time.Time) {
			ConnPoolCallback(cp.host, *path, start, err)
		}(&path, time.Now())
	}

	path = "short-circuit"

	// short-circuit available connetions.
	select {
	case rv, isopen := <-cp.connections:
		if !isopen {
			return nil, errClosedPool
		}
		return rv, nil
	default:
	}

	t := time.NewTimer(ConnPoolAvailWaitTime)
	defer t.Stop()

	// Try to grab an available connection within 1ms
	select {
	case rv, isopen := <-cp.connections:
		path = "avail1"
		if !isopen {
			return nil, errClosedPool
		}
		return rv, nil
	case <-t.C:
		// No connection came around in time, let's see
		// whether we can get one or build a new one first.
		t.Reset(d) // Reuse the timer for the full timeout.
		select {
		case rv, isopen := <-cp.connections:
			path = "avail2"
			if !isopen {
				return nil, errClosedPool
			}
			return rv, nil
		case cp.createsem <- true:
			path = "create"
			// Build a connection if we can't get a real one.
			// This can potentially be an overflow connection, or
			// a pooled connection.
			rv, err := cp.mkConn(cp.host, cp.auth)
			if err != nil {
				// On error, release our create hold
				<-cp.createsem
			}
			return rv, err
		case <-t.C:
			return nil, ErrTimeout
		}
	}
}

func (cp *connectionPool) Get() (*memcached.Client, error) {
	return cp.GetWithTimeout(ConnPoolTimeout)
}

func (cp *connectionPool) Return(c *memcached.Client) {
	if c == nil {
		return
	}

	if cp == nil {
		c.Close()
	}

	if c.IsHealthy() {
		defer func() {
			if recover() != nil {
				// This happens when the pool has already been
				// closed and we're trying to return a
				// connection to it anyway.  Just close the
				// connection.
				c.Close()
			}
		}()

		select {
		case cp.connections <- c:
		default:
			// Overflow connection.
			<-cp.createsem
			c.Close()
		}
	} else {
		<-cp.createsem
		c.Close()
	}
}

func (cp *connectionPool) StartTapFeed(args *memcached.TapArguments) (*memcached.TapFeed, error) {
	if cp == nil {
		return nil, errNoPool
	}
	mc, err := cp.Get()
	if err != nil {
		return nil, err
	}

	// A connection can't be used after TAP; Dont' count it against the
	// connection pool capacity
	<-cp.createsem

	return mc.StartTapFeed(*args)
}

const DEFAULT_WINDOW_SIZE = 20 * 1024 * 1024 // 20 Mb

func (cp *connectionPool) StartUprFeed(name string, sequence uint32) (*memcached.UprFeed, error) {
	if cp == nil {
		return nil, errNoPool
	}
	mc, err := cp.Get()
	if err != nil {
		return nil, err
	}

	// A connection can't be used after it has been allocated to UPR;
	// Dont' count it against the connection pool capacity
	<-cp.createsem

	uf, err := mc.NewUprFeed()
	if err != nil {
		return nil, err
	}

	if err := uf.UprOpen(name, sequence, DEFAULT_WINDOW_SIZE); err != nil {
		return nil, err
	}

	if err := uf.StartFeed(); err != nil {
		return nil, err
	}

	return uf, nil
}
