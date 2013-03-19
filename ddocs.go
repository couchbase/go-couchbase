package couchbase

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
