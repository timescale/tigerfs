resource "aws_cloudfront_origin_access_control" "releases" {
  name                              = "tigerfs-releases"
  description                       = "OAC for tigerfs-releases S3 bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

data "aws_cloudfront_cache_policy" "caching_optimized" {
  name = "Managed-CachingOptimized"
}

resource "aws_cloudfront_distribution" "releases" {
  enabled             = true
  default_root_object = "install.sh"
  aliases             = [var.domain_name]
  price_class         = "PriceClass_100"
  comment             = "TigerFS release binaries"

  origin {
    domain_name              = aws_s3_bucket.releases.bucket_regional_domain_name
    origin_id                = "s3-tigerfs-releases"
    origin_access_control_id = aws_cloudfront_origin_access_control.releases.id
  }

  default_cache_behavior {
    target_origin_id       = "s3-tigerfs-releases"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true
    cache_policy_id        = data.aws_cloudfront_cache_policy.caching_optimized.id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.install.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}
