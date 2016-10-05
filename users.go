package couchbase

// Return user-role data, as parsed JSON.
// Sample:
//   [{"id":"ivanivanov","name":"Ivan Ivanov","roles":[{"role":"cluster_admin"},{"bucket_name":"default","role":"bucket_admin"}]},
//    {"id":"petrpetrov","name":"Petr Petrov","roles":[{"role":"replication_admin"}]}]
func (c *Client) GetUserRoles() ([]interface{}, error) {
	ret := make([]interface{}, 0, 1)
	err := c.parseURLResponse("/settings/rbac/users", &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
