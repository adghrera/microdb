package migrate

import "testing"

func TestRenameDropRetype(t *testing.T) {
	s := Schema{Version: 3, Transforms: []Transform{
		{Op: "rename", From: "city", To: "town"},
		{Op: "drop", From: "secret"},
		{Op: "retype", From: "age", To: "int"},
	}}
	f := map[string]interface{}{"city": "Lisbon", "secret": "x", "age": "42.0"}
	if !s.Apply(f) {
		t.Fatal("apply should report change")
	}
	if f["town"] != "Lisbon" {
		t.Fatalf("rename failed: %v", f)
	}
	if _, ok := f["city"]; ok {
		t.Fatal("old field remains")
	}
	if _, ok := f["secret"]; ok {
		t.Fatal("dropped field remains")
	}
	if f["age"] != float64(42) {
		t.Fatalf("retype failed: %#v", f["age"])
	}
	if DocVer(f) != 3 {
		t.Fatalf("version not stamped: %v", f[SchemaVerField])
	}
	// Idempotent: second apply is a no-op.
	if s.Apply(f) {
		t.Fatal("re-apply must be a no-op")
	}
}

func TestPartialVersionResume(t *testing.T) {
	v1 := Schema{Version: 1, Transforms: []Transform{{Op: "rename", From: "a", To: "b"}}}
	v2 := Schema{Version: 2, Transforms: []Transform{
		{Op: "rename", From: "a", To: "b"},
		{Op: "drop", From: "c"},
	}}
	f := map[string]interface{}{"a": 1, "c": 2}
	v1.Apply(f) // -> {b:1, c:2, ver:1}
	if f["b"] == nil || f["c"] == nil || DocVer(f) != 1 {
		t.Fatalf("v1 wrong: %v", f)
	}
	// v2 must only run transform[1] (drop c) — rename already done.
	if !v2.Apply(f) {
		t.Fatal("v2 should change a v1 doc")
	}
	if _, ok := f["c"]; ok {
		t.Fatal("c should be dropped by v2")
	}
	if f["b"] != 1 {
		t.Fatalf("b must survive: %v", f)
	}
	if DocVer(f) != 2 {
		t.Fatalf("ver should be 2: %v", f)
	}
}

func TestRetypeConversions(t *testing.T) {
	cases := []struct {
		target string
		in     interface{}
		want   interface{}
		ok     bool
	}{
		{"float", "3.5", 3.5, true},
		{"float", true, 1.0, true},
		{"int", "42", float64(42), true},
		{"int", 7.9, float64(7), true},
		{"string", float64(9), "9", true},
		{"string", false, "false", true},
		{"bool", "true", true, true},
		{"bool", 0.0, false, true},
		{"int", "abc", "abc", false}, // unconvertible: left as-is
	}
	for _, c := range cases {
		got, ok := retype(c.in, c.target)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("retype(%#v -> %s) = %#v,%v want %#v,%v", c.in, c.target, got, ok, c.want, c.ok)
		}
	}
}

func TestValidateRejectsBadSchemas(t *testing.T) {
	bad := []Schema{
		{Version: 1, Transforms: []Transform{{Op: "frobnicate", From: "x"}}},
		{Version: 1, Transforms: []Transform{{Op: "rename", From: "x"}}},
		{Version: 1, Transforms: []Transform{{Op: "rename", From: "_schema_ver", To: "y"}}},
		{Version: 1, Transforms: []Transform{{Op: "retype", From: "x", To: "blob"}}},
	}
	for i, s := range bad {
		if err := s.Validate(); err == nil {
			t.Errorf("schema %d should not validate", i)
		}
	}
	good := Schema{Version: 1, Transforms: []Transform{{Op: "rename", From: "x", To: "y"}}}
	if err := good.Validate(); err != nil {
		t.Errorf("good schema rejected: %v", err)
	}
}
