package couchbase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type ViewDefinition struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce"`
}

type DDocJSON struct {
	Language string                    `json:"language"`
	Views    map[string]ViewDefinition `json:"views"`
}

type DDoc struct {
	Meta map[string]interface{} `json:"meta"`
	Json DDocJSON               `json:"json"`
}

type DDocRow struct {
	DDoc DDoc `json:"doc"`
}

type DDocsResult struct {
	Rows []DDocRow `json:"rows"`
}

// Get the design documents
func (b *Bucket) GetDDocs() (DDocsResult, error) {
	var ddocsResult DDocsResult
	err := b.pool.client.parseURLResponse(b.DDocs.URI, &ddocsResult)
	if err != nil {
		return DDocsResult{}, err
	}
	return ddocsResult, nil
}

// Install a design document.
func (b *Bucket) PutDDoc(docname string, value interface{}) error {
	u, err := b.randomBaseURL()
	if err != nil {
		return err
	}

	j, err := json.Marshal(value)
	if err != nil {
		return err
	}

	u.Path = fmt.Sprintf("/%s/_design/%s", b.Name, docname)
	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(j))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 201 {
		body, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Error installing view: %v / %s",
			res.Status, body)
	}

	return nil
}
