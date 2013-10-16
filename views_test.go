package couchbase

import (
	"testing"
)

func TestViewURL(t *testing.T) {
	b := Bucket{}
	// No URLs
	v, err := b.ViewURL("a", "b", nil)
	if err == nil {
		t.Errorf("Expected error on empty bucket, got %v", v)
	}

	// Invalidish URL
	b = Bucket{Nodes: []Node{{CouchAPIBase: "::gopher:://localhost:80x92/"}}}
	v, err = b.ViewURL("a", "b", nil)
	if err == nil {
		t.Errorf("Expected error on broken URL, got %v", v)
	}

	// Unmarshallable parameter
	b = Bucket{Nodes: []Node{{CouchAPIBase: "http:://localhost:8092/"}}}
	v, err = b.ViewURL("a", "b",
		map[string]interface{}{"ch": make(chan bool)})
	if err == nil {
		t.Errorf("Expected error on unmarshalable param, got %v", v)
	}

	tests := []struct {
		ddoc, name string
		params     map[string]interface{}
		exppath    string
		exp        map[string]string
	}{
		{"a", "b",
			map[string]interface{}{"i": 1, "b": true, "s": "ess"},
			"/x/_design/a/_view/b",
			map[string]string{"i": "1", "b": "true", "s": `"ess"`}},
		{"a", "b",
			map[string]interface{}{"unk": DocId("le"), "startkey_docid": "ess"},
			"/x/_design/a/_view/b",
			map[string]string{"unk": "le", "startkey_docid": "ess"}},
		{"a", "b",
			map[string]interface{}{"stale": "update_after"},
			"/x/_design/a/_view/b",
			map[string]string{"stale": "update_after"}},
		{"a", "b",
			map[string]interface{}{"startkey": []string{"a"}},
			"/x/_design/a/_view/b",
			map[string]string{"startkey": `["a"]`}},
		{"", "_all_docs", nil, "/x/_all_docs", map[string]string{}},
	}

	b = Bucket{Name: "x", Nodes: []Node{{CouchAPIBase: "http://localhost:8092/"}}}
	for _, test := range tests {
		us, err := b.ViewURL(test.ddoc, test.name, test.params)
		if err != nil {
			t.Errorf("Failed on %v", test)
			continue
		}

		u, err := ParseURL(us)
		if err != nil {
			t.Errorf("Failed on %v", test)
			continue
		}

		if u.Path != test.exppath {
			t.Errorf("Expected path of %v to be %v, got %v",
				test, test.exppath, u.Path)
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
