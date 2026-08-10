package kv

import "testing"

func TestGoTestInfrastructure(t *testing.T) {
	if 1+1 != 2 {
		t.Fatal("basic arithmetic failure")
	}
}
