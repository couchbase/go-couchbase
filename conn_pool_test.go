package couchbase

import (
	"io"
	"testing"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type testT struct{}

func (t testT) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (t testT) Write([]byte) (int, error) {
	return 0, io.EOF
}

func (t testT) Close() error {
	return nil
}

func testMkConn(h string, ah AuthHandler) (*memcached.Client, error) {
	return memcached.Wrap(testT{})
}

func TestConnPool(t *testing.T) {
	cp := newConnectionPool("h", &basicAuth{}, 3, 6)
	cp.mkConn = testMkConn

	seenClients := map[*memcached.Client]bool{}

	// build some connections

	for i := 0; i < 5; i++ {
		sc, err := cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		seenClients[sc] = true
	}

	if len(cp.connections) != 0 {
		t.Errorf("Expected 0 connections after gets, got %v",
			len(cp.connections))
	}

	// return them
	for k := range seenClients {
		cp.Return(k)
	}

	if len(cp.connections) != 3 {
		t.Errorf("Expected 3 connections after returning them, got %v",
			len(cp.connections))
	}

	// Try again.
	matched := 0
	grabbed := []*memcached.Client{}
	for i := 0; i < 5; i++ {
		sc, err := cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		if seenClients[sc] {
			matched++
		}
		grabbed = append(grabbed, sc)
	}

	if matched != 3 {
		t.Errorf("Expected to match 3 conns, matched %v", matched)
	}

	for _, c := range grabbed {
		cp.Return(c)
	}

	// Connect write error.
	sc, err := cp.Get()
	if err != nil {
		t.Fatalf("Error getting a connection: %v", err)
	}
	err = sc.Transmit(&gomemcached.MCRequest{})
	if err == nil {
		t.Fatalf("Expected error sending a request")
	}
	if sc.IsHealthy() {
		t.Fatalf("Expected unhealthy connection")
	}
	cp.Return(sc)

	if len(cp.connections) != 2 {
		t.Errorf("Expected to have 2 conns, have %v", len(cp.connections))
	}

	err = cp.Close()
	if err != nil {
		t.Errorf("Expected clean close, got %v", err)
	}

	err = cp.Close()
	if err == nil {
		t.Errorf("Expected error on second pool close")
	}

	sc, err = cp.Get()
	if err != closedPool {
		t.Errorf("Expected closed pool error after closed, got %v/%v", sc, err)
	}
}

func TestConnPoolNil(t *testing.T) {
	var cp *connectionPool
	c, err := cp.Get()
	if err == nil {
		t.Errorf("Expected an error getting from nil, got %v", c)
	}

	// This just shouldn't error.
	cp.Return(c)
}

func TestConnPoolClosed(t *testing.T) {
	cp := newConnectionPool("h", &basicAuth{}, 3, 6)
	cp.mkConn = testMkConn
	c, err := cp.Get()
	if err != nil {
		t.Fatal(err)
	}
	cp.Close()

	// This just shouldn't error.
	cp.Return(c)
}
