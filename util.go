package couchbase

import (
	"fmt"
	"net/url"
	"strings"
)

// Return the hostname with the given suffix removed.
func CleanupHost(h, commonSuffix string) string {
	if strings.HasSuffix(h, commonSuffix) {
		return h[:len(h)-len(commonSuffix)]
	}
	return h
}

// Find the longest common suffix from the given strings.
func FindCommonSuffix(input []string) string {
	rv := ""
	if len(input) < 2 {
		return ""
	}
	from := input
	for i := len(input[0]); i > 0; i-- {
		common := true
		suffix := input[0][i:]
		for _, s := range from {
			if !strings.HasSuffix(s, suffix) {
				common = false
				break
			}
		}
		if common {
			rv = suffix
		}
	}
	return rv
}

// Some sanity-checking around URL.Parse, which is woefully trusting of bogus URL strings
// like "" or "foo bar".
func ParseURL(urlStr string) (result *url.URL, err error) {
	result, err = url.Parse(urlStr)
	if result != nil && result.Scheme == "" {
		result = nil
		err = fmt.Errorf("Invalid URL <%s>", urlStr)
	}
	return
}
