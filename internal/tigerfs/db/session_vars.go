package db

import (
	"context"
	"sort"
)

// SessionVars holds PostgreSQL session variable names and values with
// pre-sorted keys for deterministic, allocation-free iteration at query time.
//
// Variable names must follow PostgreSQL custom GUC naming conventions
// (dotted identifiers, e.g. "app.user_id", "app.tenant_id").
//
// Create via NewSessionVars:
//
//	vars := db.NewSessionVars(map[string]string{
//	    "app.user_id":   "42",
//	    "app.tenant_id": "acme",
//	})
type SessionVars struct {
	keys   []string          // sorted once at construction
	values map[string]string // original map for value lookup
}

// NewSessionVars creates a SessionVars with pre-sorted keys.
// Returns a zero-value SessionVars if the map is nil or empty.
func NewSessionVars(m map[string]string) SessionVars {
	if len(m) == 0 {
		return SessionVars{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Copy the map so the caller can't mutate it after construction.
	values := make(map[string]string, len(m))
	for k, v := range m {
		values[k] = v
	}
	return SessionVars{keys: keys, values: values}
}

// Len returns the number of session variables.
func (sv SessionVars) Len() int {
	return len(sv.keys)
}

// Empty returns true if there are no session variables.
func (sv SessionVars) Empty() bool {
	return len(sv.keys) == 0
}

// Range calls fn for each key-value pair in sorted key order.
func (sv SessionVars) Range(fn func(key, value string)) {
	for _, k := range sv.keys {
		fn(k, sv.values[k])
	}
}

// Merge returns a new SessionVars combining base and override.
// Override values take precedence for duplicate keys. Both inputs
// are unchanged. The result has freshly sorted keys.
func (sv SessionVars) Merge(override SessionVars) SessionVars {
	if sv.Empty() {
		return override
	}
	if override.Empty() {
		return sv
	}
	merged := make(map[string]string, len(sv.values)+len(override.values))
	for k, v := range sv.values {
		merged[k] = v
	}
	for k, v := range override.values {
		merged[k] = v
	}
	return NewSessionVars(merged)
}

type sessionVarsKey struct{}

// WithSessionVars returns a derived context carrying session variables
// that will be applied via SET LOCAL to any query executed under this
// context.
//
// Calling with an empty SessionVars returns ctx unchanged.
//
// Example:
//
//	ctx = db.WithSessionVars(ctx, db.NewSessionVars(map[string]string{
//	    "app.user_id": userID,
//	}))
//	row, err := client.GetRow(ctx, schema, table, pk) // SET LOCAL applied
func WithSessionVars(ctx context.Context, vars SessionVars) context.Context {
	if vars.Empty() {
		return ctx
	}
	return context.WithValue(ctx, sessionVarsKey{}, vars)
}

// SessionVarsFromContext returns the session variables attached to ctx
// via WithSessionVars, or a zero-value SessionVars if none were set.
func SessionVarsFromContext(ctx context.Context) SessionVars {
	v, _ := ctx.Value(sessionVarsKey{}).(SessionVars)
	return v
}
