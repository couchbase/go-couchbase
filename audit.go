package couchbase

import ()

// Sample data:
// {"disabled":[],"uid":"61631328","auditdEnabled":true,"disabledUsers":[],
//  "logPath":"/Users/johanlarson/Library/Application Support/Couchbase/var/lib/couchbase/logs",
//  "rotateInterval":86400,"rotateSize":20971520}
type AuditSpec struct {
	Disabled       []uint32 `json:"disabled"`
	Uid            string   `json:"uid"`
	AuditdEnabled  bool     `json:"auditdEnabled`
	DisabledUsers  []string `json:"disabledUsers"`
	LogPath        string   `json:"logPath"`
	RotateInterval int64    `json:"rotateInterval"`
	RotateSize     int64    `json:"rotateSize"`
}

func (c *Client) GetAuditSpec() (*AuditSpec, error) {
	ret := &AuditSpec{}
	err := c.parseURLResponse("/settings/audit", ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
