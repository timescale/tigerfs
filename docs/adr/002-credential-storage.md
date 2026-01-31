# ADR-002: Credential Storage Strategy

**Date:** 2026-01-23
**Status:** Accepted
**Deciders:** Mike Freedman

## Context

TigerFS needs to handle PostgreSQL database credentials across diverse deployment environments:

| Environment | Keyring Available? |
|-------------|-------------------|
| macOS/Windows Desktop | Yes |
| Linux Desktop | Conditional (needs D-Bus) |
| Linux Server/Headless | No |
| Docker/Kubernetes | No |
| CI/CD (GitHub Actions) | No |

Unlike CLI tools targeting only developer laptops, TigerFS must work reliably in containers, CI/CD pipelines, and production servers where system keyrings aren't available.

Users also expect standard PostgreSQL credential handling patterns they already know.

## Decision

Use a **layered credential resolution** strategy that prioritizes standard PostgreSQL conventions over platform-specific features.

**Resolution order (highest to lowest precedence):**

1. Password in connection string (if present)
2. `PGPASSWORD` or `TIGERFS_PASSWORD` environment variable
3. `password_command` config option (for secret managers)
4. `~/.pgpass` file (automatic via pgx library)

**Explicitly NOT implemented:**
- System keyring integration (unreliable across target platforms)
- Plain-text password in config file (security risk)

### password_command Examples

```yaml
# HashiCorp Vault
password_command: "vault kv get -field=password secret/tigerfs/prod"

# AWS Secrets Manager
password_command: "aws secretsmanager get-secret-value --secret-id prod-db --query SecretString --output text"

# 1Password CLI
password_command: "op read op://vault/tigerfs/password"
```

## Consequences

### Positive

- Works on all target platforms without conditional logic
- Follows PostgreSQL conventions (familiar to users)
- `password_command` enables enterprise secret manager integration
- No dependency on platform-specific keyring libraries
- Simple implementation (~90 lines in `internal/tigerfs/db/password.go`)

### Negative

- Desktop users don't get automatic keyring integration
- Users must configure `.pgpass` or environment variables explicitly
- `password_command` doesn't handle quoted arguments (simple space-split parsing)

### Neutral

- Keyring support could be added later as opt-in if demand exists

## Implementation

- `internal/tigerfs/db/password.go` - Password resolution logic
- `internal/tigerfs/config/config.go` - `PasswordCommand` config field
