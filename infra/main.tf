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
