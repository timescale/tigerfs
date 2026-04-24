package db

import "context"

// SessionVars holds PostgreSQL session variable names and values.
// Variables are applied via SET LOCAL (set_config with is_local=true)
// within a transaction, scoping them to the current query.
//
// Variable names must follow PostgreSQL custom GUC naming conventions
// (dotted identifiers, e.g. "app.user_id", "app.tenant_id").
//
// Example:
//
//	db.SessionVars{
//	    "app.user_id":   "42",
//	    "app.tenant_id": "acme",
//	}
type SessionVars map[string]string

type sessionVarsKey struct{}

// WithSessionVars returns a derived context carrying session variables
// that will be applied via SET LOCAL to any query executed under this
// context. Requires the Client to have session scoping enabled.
//
// Calling with an empty or nil map returns ctx unchanged.
//
// Example:
//
//	ctx = db.WithSessionVars(ctx, db.SessionVars{"app.user_id": userID})
//	row, err := client.GetRow(ctx, schema, table, pk) // SET LOCAL applied
func WithSessionVars(ctx context.Context, vars SessionVars) context.Context {
	if len(vars) == 0 {
		return ctx
	}
	return context.WithValue(ctx, sessionVarsKey{}, vars)
}

// SessionVarsFromContext returns the session variables attached to ctx
// via WithSessionVars, or nil if none were set.
func SessionVarsFromContext(ctx context.Context) SessionVars {
	v, _ := ctx.Value(sessionVarsKey{}).(SessionVars)
	return v
}
