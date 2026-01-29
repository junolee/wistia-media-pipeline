#=====================================
# Terraform + Provider
#=====================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      project = "wistia"
    }
  }
}

#=====================================
# S3
#=====================================

resource "aws_s3_bucket" "bucket" {
  bucket = "jl-wistia-pipeline"
}

resource "aws_s3_object" "folders" {
  for_each = toset([
	  "raw/",
    "state/",
    "lambda/",
	  "tables/events/",
	  "tables/media/",
	  "tables/visitors/",
    "jobs/scripts/",
	  "jobs/libs/",
	  "jobs/logs/spark-ui/",
	  "jobs/tmp/glue/",
	])
  bucket = aws_s3_bucket.bucket.id
  key    = each.key
}
#=====================================
# IAM Role for GitHub Actions
#=====================================
locals {
  github_oidc_provider_arn = "arn:aws:iam::423623837966:oidc-provider/token.actions.githubusercontent.com"
}

resource "aws_iam_role" "github_actions_role" {
  name = "ws-github-actions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRoleWithWebIdentity"
        Principal = {
          Federated = local.github_oidc_provider_arn
        }
        Condition = {
          StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub" = "repo:junolee/wistia-media-pipeline:*"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "github_actions_policy" {
  name        = "ws-s3-policy"
  description = "Minimal S3 access for GitHub Actions to ws-wistia-pipeline"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = "s3:ListBucket"
        Resource = "arn:aws:s3:::jl-wistia-pipeline"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
        ]
        Resource = "arn:aws:s3:::jl-wistia-pipeline/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_actions_role_policy_attach" {
  role       = aws_iam_role.github_actions_role.name
  policy_arn = aws_iam_policy.github_actions_policy.arn
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions_role.arn
}

