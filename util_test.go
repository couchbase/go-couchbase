package couchbase

import (
	"testing"
)

func TestCleanupHostEmpty(t *testing.T) {
	assert(t, "empty", CleanupHost("", ""), "")
	assert(t, "empty suffix", CleanupHost("aprefix", ""), "aprefix")
	assert(t, "empty host", CleanupHost("", "asuffix"), "")
	assert(t, "matched suffix",
		CleanupHost("server1.example.com:11210", ".example.com:11210"),
		"server1")
}
