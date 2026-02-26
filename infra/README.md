# tigerfs-releases

Terraform config for the TigerFS release pipeline: S3 + CloudFront CDN serving binaries at `install.tigerfs.io`.

## Architecture

```
install.tigerfs.io
        │
   Route 53 (A alias, delegated from Namecheap via NS records)
        │
   CloudFront (PriceClass_100, HTTPS, CachingOptimized)
        │
   S3 (tigerfs-releases, private, OAC)
        │
        ├── install.sh          ← default root object
        ├── latest.txt          ← current version tag
        └── releases/v0.5.0/   ← per-tag archives + checksums
```

## Resources

| File | Resources |
|------|-----------|
| `main.tf` | S3 bucket, public access block, bucket policy (CloudFront OAC) |
| `cdn.tf` | CloudFront distribution, origin access control, cache policy |
| `cert.tf` | ACM certificate (DNS-validated) |
| `dns.tf` | Route 53 hosted zone, ACM validation records, cert validation waiter, A-alias to CloudFront |
| `iam.tf` | CI user (`tigerfs-releases-ci`) with S3 PutObject + CloudFront invalidation |
| `variables.tf` | `domain_name` (default: `install.tigerfs.io`) |
| `outputs.tf` | CloudFront ID, domain, nameservers, CI credentials |

## Usage

```bash
terraform init
terraform plan
terraform apply
```

### First-time setup

After the first `terraform apply`, add NS records for `install.tigerfs.io` at Namecheap. The nameservers are in the output:

```bash
terraform output nameservers
```

This is a one-time delegation step. Once delegated, Route 53 handles everything (ACM validation, CloudFront alias).

### GitHub Secrets

Set these in the tigerfs repo from `terraform output`:

| Secret | Source |
|--------|--------|
| `AWS_ACCESS_KEY_ID` | `ci_access_key_id` |
| `AWS_SECRET_ACCESS_KEY` | `terraform output -raw ci_secret_access_key` |
| `CLOUDFRONT_DISTRIBUTION_ID` | `cloudfront_distribution_id` |

## S3 layout

GoReleaser uploads on `v*` tags:

```
s3://tigerfs-releases/
├── install.sh
├── latest.txt
└── releases/
    └── v0.5.0/
        ├── tigerfs_Darwin_arm64.tar.gz
        ├── tigerfs_Darwin_arm64.tar.gz.sha256
        ├── tigerfs_Darwin_x86_64.tar.gz
        ├── tigerfs_Darwin_x86_64.tar.gz.sha256
        ├── tigerfs_Linux_arm64.tar.gz
        ├── tigerfs_Linux_arm64.tar.gz.sha256
        ├── tigerfs_Linux_x86_64.tar.gz
        └── tigerfs_Linux_x86_64.tar.gz.sha256
```
