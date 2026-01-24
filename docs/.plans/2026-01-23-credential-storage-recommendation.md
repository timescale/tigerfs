# Credential Storage Recommendation for DBFS

## Platform Support Analysis

### Keyring Availability Summary

| Platform | Keyring Works? | Primary Use Case |
|----------|---------------|------------------|
| macOS Desktop | ✅ Yes | Developer machines |
| Windows Desktop | ✅ Yes | Developer machines |
| Linux Desktop | ⚠️ Conditional | Requires desktop environment |
| Linux Server/Headless | ❌ No | No D-Bus/keyring services |
| Docker Containers | ❌ No | Isolated, no keyring daemon |
| Firecracker MicroVMs | ❌ No | Minimal environment |
| GitHub Actions | ❌ No | Headless CI runners |
| Kubernetes Pods | ❌ No | Container environment |

**Conclusion:** Keyring only reliable on desktop developer machines (macOS/Windows, some Linux).

## Tiger CLI vs DBFS Use Cases

### Tiger CLI Context
- Primary users: Developers on laptops
- One-time `tiger auth login` flow
- Managed cloud service credentials (API keys)
- Keyring default makes sense - fails gracefully to config file

### DBFS Context
- **Multiple deployment targets:**
  - Developer machines (like Tiger CLI)
  - Production servers (headless)
  - Containers (Docker, Kubernetes)
  - CI/CD pipelines (GitHub Actions)
  - Firecracker MicroVMs
- Database credentials (not API keys)
- Standard PostgreSQL connection patterns expected
- Must work reliably across all environments

## Recommended Credential Storage Strategy

### Configuration Hierarchy (Precedence: Low to High)

1. **Config file** (`~/.config/dbfs/config.yaml`)
2. **Environment variables** (`PGHOST`, `PGUSER`, `PGPASSWORD`, `DBFS_*`)
3. **PostgreSQL .pgpass** (automatic, checked by Go pgx library)
4. **password_command** (if specified in config)
5. **Command-line flags** (override everything)

### Password Storage Options

#### Option 1: No Password in Config (Recommended Default)

```yaml
# ~/.config/dbfs/config.yaml
connection:
  host: localhost
  user: myuser
  database: mydb
  # No password - use .pgpass or env var
```

**Password provided via:**
- `~/.pgpass` file (PostgreSQL standard, mode 0600) - RECOMMENDED
- `PGPASSWORD` environment variable
- Interactive prompt
- `password_command` (for secret managers)

#### Option 2: password_command (Enterprise/Secret Manager Integration)

```yaml
# ~/.config/dbfs/config.yaml
connection:
  host: prod.example.com
  user: readonly
  database: myapp
  password_command: "vault kv get -field=password secret/prod-db"
  # Or: "pass show dbfs/prod"
  # Or: "aws secretsmanager get-secret-value --secret-id dbfs-prod --query SecretString --output text"
  # Or: "kubectl get secret dbfs-creds -o jsonpath='{.data.password}' | base64 -d"
```

Command output (stdout) used as password. Supports any secret management tool.

#### Option 3: Keyring Support (Optional Enhancement)

```yaml
# ~/.config/dbfs/config.yaml
connection:
  host: localhost
  user: myuser
  database: mydb
  password_storage: keyring  # Options: keyring, pgpass, file, none
```

**Only enable keyring if:**
- Using Go keyring library (99designs/keyring or similar)
- Automatic fallback to file when keyring unavailable
- Clear error messages when keyring not available
- NOT the default (since it fails in most deployment environments)

#### Option 4: Plain Password in Config (Discouraged, With Warnings)

```yaml
# ~/.config/dbfs/config.yaml
connection:
  host: localhost
  user: myuser
  database: mydb
  password: "secret123"  # ⚠️ DISCOURAGED
```

**If allowed:**
- Print warning on mount: "⚠️  Password stored in plain text config file"
- Require file mode 0600 (readable only by owner)
- Document risks (accidental git commit, etc.)

## Implementation Recommendation

### Phase 1: Core Support (MVP)
1. **.pgpass file** (automatic via pgx library) ✅
2. **Environment variables** (`PGPASSWORD`, `DBFS_PASSWORD`) ✅
3. **Interactive prompt** (if password not found) ✅
4. **Connection string** (password in URL, discouraged but works) ✅

### Phase 2: Enterprise Features
5. **password_command** (secret manager integration) ✅
6. **Config file password** (plain text, with warnings) ⚠️

### Phase 3: Optional Enhancement (If Demand Exists)
7. **Keyring support** (desktop convenience, with fallback) 🔧

## Precedence Rules

**Password resolution order:**
1. `password_command` output (if specified)
2. Connection string password (if present)
3. `DBFS_PASSWORD` or `PGPASSWORD` environment variable
4. Keyring (if enabled and available)
5. `~/.pgpass` file
6. Config file password (if present)
7. Interactive prompt (TTY available)
8. Error: No password available

## Environment-Specific Best Practices

### Developer Machines
```bash
# Option 1: .pgpass (recommended)
echo "localhost:5432:mydb:myuser:secret123" >> ~/.pgpass
chmod 0600 ~/.pgpass
dbfs /mnt/db

# Option 2: Environment variable
export PGPASSWORD='secret123'
dbfs postgres://localhost/mydb /mnt/db
```

### Docker Containers
```bash
# Option 1: Environment variables (best for containers)
docker run -e PGHOST=postgres -e PGUSER=app -e PGPASSWORD=secret dbfs-image

# Option 2: Mount .pgpass
docker run -v ~/.pgpass:/root/.pgpass:ro dbfs-image

# Option 3: Docker secrets (Swarm)
echo "secret123" | docker secret create db_password -
docker service create --secret db_password dbfs-image
```

### Kubernetes
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: dbfs-creds
stringData:
  PGPASSWORD: "secret123"
---
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: dbfs
    envFrom:
    - secretRef:
        name: dbfs-creds
    # Or use password_command: kubectl get secret...
```

### CI/CD (GitHub Actions)
```yaml
- name: Mount Database
  env:
    PGHOST: postgres
    PGUSER: test
    PGPASSWORD: ${{ secrets.DB_PASSWORD }}
  run: dbfs /mnt/db
```

### Secret Managers
```yaml
# AWS Secrets Manager
connection:
  password_command: "aws secretsmanager get-secret-value --secret-id prod-db --query SecretString --output text"

# HashiCorp Vault
connection:
  password_command: "vault kv get -field=password secret/dbfs/prod"

# Pass (Unix password manager)
connection:
  password_command: "pass show dbfs/production"
```

## Security Documentation

### User-Facing Documentation

```markdown
## Credential Security

⚠️ **Never commit passwords to version control or store in plain text config files in production.**

### Recommended Approaches (Most to Least Secure)

1. **~/.pgpass file** (PostgreSQL standard)
   ```bash
   # Create ~/.pgpass
   echo "hostname:port:database:username:password" >> ~/.pgpass
   chmod 0600 ~/.pgpass
   ```
   Automatic, secure file permissions required.

2. **password_command with secret manager**
   ```yaml
   password_command: "vault kv get -field=password secret/prod-db"
   ```
   Best for production. Integrates with enterprise secret management.

3. **Environment variables** (development/containers)
   ```bash
   export PGPASSWORD='secret'
   dbfs /mnt/db
   ```
   Good for containers, CI/CD. Not visible in process list.

4. **Interactive prompt** (manual workflows)
   ```bash
   dbfs postgres://user@host/db /mnt/db
   Password: ****
   ```
   Safe for ad-hoc usage.

### ⛔ Avoid These

- Passwords in command-line arguments (visible in `ps`)
- Passwords in connection strings (logged, visible)
- Passwords in config files without 0600 permissions
- Passwords committed to git
```

## Why Not Default to Keyring?

**Tiger CLI can default to keyring because:**
- Primary users are developers on desktops
- Fallback to config file works for their use case
- One-time `auth login` flow
- Managed cloud service (limited credential types)

**DBFS cannot default to keyring because:**
- Many target environments lack keyring support
- Must work in containers, CI/CD, servers
- Standard PostgreSQL credential patterns expected
- Keyring failure in production = outage

**Keyring as opt-in enhancement = acceptable**
- Convenience for desktop users
- Automatic fallback to standard methods
- Not required for any workflow

## Conclusion

**Recommended implementation:**
1. Support standard PostgreSQL credential methods (.pgpass, env vars)
2. Add password_command for secret manager integration
3. Optionally add keyring as opt-in convenience feature (not default)
4. Document security best practices clearly

This approach:
- ✅ Works on all target platforms
- ✅ Follows PostgreSQL conventions
- ✅ Supports enterprise secret management
- ✅ Optional desktop convenience via keyring
- ✅ Clear security guidance
