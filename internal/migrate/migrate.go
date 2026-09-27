// Package migrate implements online schema migration: versioned,
// per-collection field transforms that are applied lazily on read,
// so a rename or retype never requires a stop-the-world rewrite.
//
// Each collection has a schema version and an ordered list of
// transforms. A document carries the schema version it was last
// migrated to in the reserved field "_schema_ver". On read, any
// document behind the current schema is passed through the newer
// transforms exactly once (the marker is stamped in-memory), and the
// transforms are idempotent by construction: once a doc carries the
// current version it is never re-transformed.
//
// Because the marker lives in the document's own fields, migration
// state replicates with the data (LWW) and survives restarts. A
// background "persist" pass (store.MigrateAll) can write the
// transformed docs durably when you want the rewrite to happen on
// your schedule instead of lazily forever.
package migrate

import "fmt"

// SchemaVerField is the reserved document field that records the
// schema version a document has been migrated through.
const SchemaVerField = "_schema_ver"

// Transform is one schema operation. Kinds:
//
//	rename: move the value at From to To (no-op if From absent)
//	drop:   remove the field named From
//	retype: convert the field named From to a target type:
//	        "float" | "int" | "string" | "bool"
//	        (best-effort: unconvertible values are left as-is)
type Transform struct {
	Op   string `json:"op"`
	From string `json:"from"`
	To   string `json:"to,omitempty"`
}

// Schema is the migration state for one collection.
type Schema struct {
	Version    int         `json:"version"`
	Transforms []Transform `json:"transforms"` // transforms[i] migrates version i -> i+1
}

// Validate checks the transform list is well-formed.
func (s Schema) Validate() error {
	for i, t := range s.Transforms {
		switch t.Op {
		case "rename":
			if t.From == "" || t.To == "" {
				return fmt.Errorf("transform %d: rename requires from and to", i)
			}
			if t.From == SchemaVerField || t.To == SchemaVerField {
				return fmt.Errorf("transform %d: %s is reserved", i, SchemaVerField)
			}
		case "drop":
			if t.From == "" {
				return fmt.Errorf("transform %d: drop requires from", i)
			}
			if t.From == SchemaVerField {
				return fmt.Errorf("transform %d: %s is reserved", i, SchemaVerField)
			}
		case "retype":
			if t.From == "" {
				return fmt.Errorf("transform %d: retype requires from", i)
			}
			switch t.To {
			case "float", "int", "string", "bool":
			default:
				return fmt.Errorf("transform %d: retype target must be float|int|string|bool, got %q", i, t.To)
			}
		default:
			return fmt.Errorf("transform %d: unknown op %q (rename|drop|retype)", i, t.Op)
		}
	}
	return nil
}

// DocVer returns the schema version a document's fields carry
// (0 = never migrated).
func DocVer(fields map[string]interface{}) int {
	switch v := fields[SchemaVerField].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// Apply runs transforms[fromVer : schema.Version] against the fields
// in place and stamps the new version. Returns true if the fields
// were changed. Transforms at indices < fromVer are skipped (already
// applied), which is what makes the operation idempotent.
func (s Schema) Apply(fields map[string]interface{}) bool {
	from := DocVer(fields)
	if from >= s.Version {
		return false
	}
	for i := from; i < s.Version && i < len(s.Transforms); i++ {
		applyOne(s.Transforms[i], fields)
	}
	fields[SchemaVerField] = float64(s.Version)
	return true
}

func applyOne(t Transform, fields map[string]interface{}) {
	switch t.Op {
	case "rename":
		if v, ok := fields[t.From]; ok {
			delete(fields, t.From)
			fields[t.To] = v
		}
	case "drop":
		delete(fields, t.From)
	case "retype":
		v, ok := fields[t.From]
		if !ok {
			return
		}
		if cv, ok := retype(v, t.To); ok {
			fields[t.From] = cv
		}
	}
}

func retype(v interface{}, target string) (interface{}, bool) {
	switch target {
	case "float":
		switch x := v.(type) {
		case float64:
			return x, true
		case int:
			return float64(x), true
		case string:
			var f float64
			if _, err := fmt.Sscanf(x, "%g", &f); err == nil {
				return f, true
			}
		case bool:
			if x {
				return 1.0, true
			}
			return 0.0, true
		}
	case "int":
		switch x := v.(type) {
		case float64:
			return float64(int64(x)), true
		case int:
			return float64(x), true
		case string:
			var n int64
			if _, err := fmt.Sscanf(x, "%d", &n); err == nil {
				return float64(n), true
			}
		}
	case "string":
		switch x := v.(type) {
		case string:
			return x, true
		case float64:
			if x == float64(int64(x)) {
				return fmt.Sprintf("%d", int64(x)), true
			}
			return fmt.Sprintf("%g", x), true
		case bool:
			if x {
				return "true", true
			}
			return "false", true
		}
	case "bool":
		switch x := v.(type) {
		case bool:
			return x, true
		case float64:
			return x != 0, true
		case string:
			switch x {
			case "true", "1":
				return true, true
			case "false", "0":
				return false, true
			}
		}
	}
	return v, false
}
