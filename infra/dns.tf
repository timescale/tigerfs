resource "aws_route53_zone" "install" {
  name = var.domain_name   # install.tigerfs.io
}

# ACM DNS validation record
resource "aws_route53_record" "acm_validation" {
  for_each = {
    for dvo in aws_acm_certificate.install.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  }

  zone_id = aws_route53_zone.install.zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 300
  records = [each.value.record]
}

# Wait for cert validation before CloudFront can use it
resource "aws_acm_certificate_validation" "install" {
  certificate_arn         = aws_acm_certificate.install.arn
  validation_record_fqdns = [for r in aws_route53_record.acm_validation : r.fqdn]
}

# Alias record: install.tigerfs.io -> CloudFront
resource "aws_route53_record" "cdn" {
  zone_id = aws_route53_zone.install.zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.releases.domain_name
    zone_id                = aws_cloudfront_distribution.releases.hosted_zone_id
    evaluate_target_health = false
  }
}
