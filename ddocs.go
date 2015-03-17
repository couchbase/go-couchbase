package couchbase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// ViewDefinition represents a single view within a design document.
type ViewDefinition struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

// DDoc is the document body of a design document specifying a view.
type DDoc struct {
	Language string                    `json:"language,omitempty"`
	Views    map[string]ViewDefinition `json:"views"`
}

// DDocsResult represents the result from listing the design
// documents.
type DDocsResult struct {
	Rows []struct {
		DDoc struct {
			Meta map[string]interface{}
			JSON DDoc
		} `json:"doc"`
	} `json:"rows"`
}

// GetDDocs lists all design documents
func (b *Bucket) GetDDocs() (DDocsResult, error) {
	var ddocsResult DDocsResult

	err := b.pool.client.parseURLResponse(b.DDocs.URI, &ddocsResult)
	if err != nil {
		return DDocsResult{}, err
	}
	return ddocsResult, nil
}

func (b *Bucket) GetDDocWithRetry(docname string, into interface{}) error {

	ddocURI := fmt.Sprintf("/%s/_design/%s", b.Name, docname)
	err := b.parseAPIResponse(ddocURI, &into)
	if err != nil {
		return err
	}
	return nil
}

func (b *Bucket) GetDDocsWithRetry() (DDocsResult, error) {
	var ddocsResult DDocsResult

	err := b.parseURLResponse(b.DDocs.URI, &ddocsResult)
	if err != nil {
		return DDocsResult{}, err
	}
	return ddocsResult, nil
}

func (b *Bucket) ddocURL(docname string) (string, error) {
	u, err := b.randomBaseURL()
	if err != nil {
		return "", err
	}
	u.Path = fmt.Sprintf("/%s/_design/%s", b.Name, docname)
	return u.String(), nil
}

// PutDDoc installs a design document.
func (b *Bucket) PutDDoc(docname string, value interface{}) error {

	var Err error

	nodes := b.Nodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no couch rest URLs")
	}
	maxRetries := len(nodes)

	for retryCount := 0; retryCount < maxRetries; retryCount++ {

		Err = nil
		ddocU, err := b.ddocURL(docname)
		if err != nil {
			return err
		}

		j, err := json.Marshal(value)
		if err != nil {
			return err
		}

		req, err := http.NewRequest("PUT", ddocU, bytes.NewReader(j))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		err = maybeAddAuth(req, b.authHandler())
		if err != nil {
			return err
		}

		res, err := HTTPClient.Do(req)
		if err != nil {
			return err
		}

		if res.StatusCode != 201 {
			body, _ := ioutil.ReadAll(res.Body)
			Err = fmt.Errorf("error installing view: %v / %s",
				res.Status, body)
			log.Printf(" Error in PutDDOC %v. Retrying...", Err)
			res.Body.Close()
			b.Refresh()
			continue
		}

		res.Body.Close()
		break
	}

	return Err
}

// GetDDoc retrieves a specific a design doc.
func (b *Bucket) GetDDoc(docname string, into interface{}) error {
	var Err error
	var res *http.Response

	nodes := b.Nodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no couch rest URLs")
	}
	maxRetries := len(nodes)

	for retryCount := 0; retryCount < maxRetries; retryCount++ {

		Err = nil
		ddocU, err := b.ddocURL(docname)
		if err != nil {
			return err
		}

		req, err := http.NewRequest("GET", ddocU, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		err = maybeAddAuth(req, b.authHandler())
		if err != nil {
			return err
		}

		res, err = HTTPClient.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode != 200 {
			body, _ := ioutil.ReadAll(res.Body)
			Err = fmt.Errorf("error reading view: %v / %s",
				res.Status, body)
			log.Printf(" Error in GetDDOC %v Retrying...", Err)
			b.Refresh()
			res.Body.Close()
			continue
		}
		res.Body.Close()
		break
	}

	if Err != nil {
		return Err
	}

	d := json.NewDecoder(res.Body)
	return d.Decode(into)
}

// DeleteDDoc removes a design document.
func (b *Bucket) DeleteDDoc(docname string) error {

	var Err error
	nodes := b.Nodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no couch rest URLs")
	}
	maxRetries := len(nodes)

	for retryCount := 0; retryCount < maxRetries; retryCount++ {

		Err = nil
		ddocU, err := b.ddocURL(docname)
		if err != nil {
			return err
		}

		req, err := http.NewRequest("DELETE", ddocU, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		err = maybeAddAuth(req, b.authHandler())
		if err != nil {
			return err
		}

		res, err := HTTPClient.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode != 200 {
			body, _ := ioutil.ReadAll(res.Body)
			Err = fmt.Errorf("error deleting view : %v / %s", res.Status, body)
			log.Printf(" Error in DeleteDDOC %v. Retrying ... ", Err)
			b.Refresh()
			res.Body.Close()
			continue
		}

		res.Body.Close()
		break
	}
	return Err
}
