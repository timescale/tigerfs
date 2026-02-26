output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID (for GitHub Secrets: CLOUDFRONT_DISTRIBUTION_ID)"
  value       = aws_cloudfront_distribution.releases.id
}

output "cloudfront_domain_name" {
  description = "CloudFront domain name"
  value       = aws_cloudfront_distribution.releases.domain_name
}

output "nameservers" {
  description = "NS records to add at Namecheap for install.tigerfs.io delegation"
  value       = aws_route53_zone.install.name_servers
}

output "ci_access_key_id" {
  description = "AWS access key ID for GitHub Actions (GitHub Secret: AWS_ACCESS_KEY_ID)"
  value       = aws_iam_access_key.ci.id
}

output "ci_secret_access_key" {
  description = "AWS secret access key for GitHub Actions (GitHub Secret: AWS_SECRET_ACCESS_KEY)"
  value       = aws_iam_access_key.ci.secret
  sensitive   = true
}
