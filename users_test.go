package couchbase

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUserUnmarshal(t *testing.T) {
	text := `[{"id":"ivanivanov","name":"Ivan Ivanov","roles":[{"role":"cluster_admin"},{"bucket_name":"default","role":"bucket_admin"}]},
			{"id":"petrpetrov","name":"Petr Petrov","roles":[{"role":"replication_admin"}]}]`
	users := make([]User, 0)

	err := json.Unmarshal([]byte(text), &users)
	if err != nil {
		t.Fatalf("Unable to unmarshal: %v", err)
	}

	expected := []User{
		User{Id: "ivanivanov", Name: "Ivan Ivanov", Roles: []Role{
			Role{Role: "cluster_admin"},
			Role{Role: "bucket_admin", BucketName: "default"}}},
		User{Id: "petrpetrov", Name: "Petr Petrov", Roles: []Role{
			Role{Role: "replication_admin"}}},
	}
	if !reflect.DeepEqual(users, expected) {
		t.Fatalf("Unexpected unmarshalled result. Expected %v, got %v.", expected, users)
	}

	ivanRoles := rolesToParamFormat(users[0].Roles)
	ivanRolesExpected := "cluster_admin,bucket_admin[default]"
	if ivanRolesExpected != ivanRoles {
		t.Errorf("Unexpected param for Ivan. Expected %v, got %v.", ivanRolesExpected, ivanRoles)
	}
	petrRoles := rolesToParamFormat(users[1].Roles)
	petrRolesExpected := "replication_admin"
	if petrRolesExpected != petrRoles {
		t.Errorf("Unexpected param for Petr. Expected %v, got %v.", petrRolesExpected, petrRoles)
	}

}
