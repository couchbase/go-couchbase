package couchbase

import (
	"net/url"
	"testing"
)

func TestViewURL(t *testing.T) {
	tests := []struct {
		ddoc, name string
		params     map[string]interface{}
		exp        map[string]string
	}{
		{"a", "b",
			map[string]interface{}{"i": 1, "b": true, "s": "ess"},
			map[string]string{"i": "1", "b": "true", "s": `"ess"`}},
		{"a", "b",
			map[string]interface{}{"unk": DocId("le"), "startkey_docid": "ess"},
			map[string]string{"unk": "le", "startkey_docid": "ess"}},
		{"a", "b",
			map[string]interface{}{"stale": "update_after"},
			map[string]string{"stale": "update_after"}},
	}

	b := Bucket{Nodes: []Node{{CouchAPIBase: "http://localhost:8092/"}}}
	for _, test := range tests {
		us, err := b.ViewURL(test.ddoc, test.name, test.params)
		if err != nil {
			t.Errorf("Failed on %v", test)
			continue
		}

		u, err := url.Parse(us)
		if err != nil {
			t.Errorf("Failed on %v", test)
			continue
		}

		got := u.Query()

		if len(got) != len(test.exp) {
			t.Errorf("Expected %v, got %v", test.exp, got)
			continue
		}

		for k, v := range test.exp {
			if len(got[k]) != 1 || got.Get(k) != v {
				t.Errorf("Expected param %v to be %q on %v, was %#q",
					k, v, test, got[k])
			}
		}
	}
}
