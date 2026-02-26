resource "aws_iam_user" "ci" {
  name = "tigerfs-releases-ci"
}

resource "aws_iam_user_policy" "ci" {
  name = "tigerfs-releases-ci"
  user = aws_iam_user.ci.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3Write"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.releases.arn}/*"
      },
      {
        Sid      = "CloudFrontInvalidate"
        Effect   = "Allow"
        Action   = "cloudfront:CreateInvalidation"
        Resource = aws_cloudfront_distribution.releases.arn
      }
    ]
  })
}

resource "aws_iam_access_key" "ci" {
  user = aws_iam_user.ci.name
}
