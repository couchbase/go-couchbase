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

func TestFindCommonSuffix(t *testing.T) {
	assert(t, "empty", FindCommonSuffix([]string{}), "")
	assert(t, "one", FindCommonSuffix([]string{"blah"}), "")
	assert(t, "two", FindCommonSuffix([]string{"blah.com", "foo.com"}), ".com")
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		in    string
		works bool
	}{
		{"", false},
		{"http://whatever/", true},
		{"http://%/", false},
	}

	for _, test := range tests {
		got, err := ParseURL(test.in)
		switch {
		case err == nil && test.works,
			!(err == nil || test.works):
		case err == nil && !test.works:
			t.Errorf("Expected failure on %v, got %v", test.in, got)
		case test.works && err != nil:
			t.Errorf("Expected success on %v, got %v", test.in, err)
		}
	}
}
