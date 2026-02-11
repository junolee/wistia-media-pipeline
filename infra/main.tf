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
          "token.actions.githubusercontent.com:sub" = "repo:junolee/wistia-media-pipeline:ref:refs/heads/main"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "github_actions_policy" {
  name        = "ws-github-actions-policy"
  description = "Access for GitHub Actions to s3 and lambda"

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
      },
      {
        Effect = "Allow",
        Action = [
          "lambda:UpdateFunctionCode",
          "lambda:GetFunction",
          "lambda:ListFunctions"
        ],
        Resource = "arn:aws:lambda:us-east-1:423623837966:function:wistia_to_s3"
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

#=====================================
# Lambda
#=====================================

data "aws_iam_policy_document" "assume_lambda_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name = "ws_lambda_exec_role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda_role.json
}

locals {
  lambda_permissions = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/SecretsManagerReadWrite",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  ]
}

resource "aws_iam_role_policy_attachment" "lambda_role_policy_attach" {
  role       = aws_iam_role.lambda_role.name
  for_each   = toset(local.lambda_permissions)
  policy_arn = each.value
}

# Lambda function

resource "aws_lambda_function" "ingest" {
  function_name = "wistia_to_s3"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900

  s3_bucket = "jl-wistia-pipeline"
  s3_key    = "lambda/ingest_lambda.zip"

  environment {
    variables = {
      YT_MEDIA_ID = "v08dlrgr7v"
      FB_MEDIA_ID = "gskhw4w4lm"
      API_TOKEN_SECRET_NAME = "wistia-api-token"
      BUCKET_NAME = "jl-wistia-pipeline"
      RAW_PREFIX = "raw"
      WISTIA_CHECKPOINT_PATH = "state/wistia_checkpoint.json"
      DRY_RUN = "false"
    }
  }
}

#=====================================
# IAM for Glue Job
#=====================================

data "aws_iam_policy_document" "glue_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "ws-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_policy.json
}

locals {
  glue_permissions = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
  ]
}

resource "aws_iam_role_policy_attachment" "glue_role_attachment" {
  for_each   = toset(local.glue_permissions)
  role       = aws_iam_role.glue_role.name
  policy_arn = each.value
}


#=====================================
# Glue Job
#=====================================


resource "aws_glue_job" "jobs" {
  name     = "ws-raw-to-curated"
  role_arn = aws_iam_role.glue_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bucket.bucket}/jobs/scripts/run.py"
    python_version  = "3"
  }

  default_arguments = {
      "--enable-glue-datacatalog"          = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-continuous-log-filter"     = "true"
      "--enable-job-insights"              = "true"
      "--enable-spark-ui"                  = "true"
      "--enable-metrics"                   = "true"

      "--job-language"                     = "python"
      "--job-bookmark-option"              = "job-bookmark-disable"

      "--TempDir"                          = "s3://${aws_s3_bucket.bucket.bucket}/jobs/tmp/glue/"
      "--spark-event-logs-path"            = "s3://${aws_s3_bucket.bucket.bucket}/jobs/logs/spark-ui/"

      "--JOB_NAME"      = "ws-raw-to-curated"
      "--PIPELINE_MODE"    = "full"
      "--START_DATE"       = "2024-01-01"
      "--SOURCE_DB"        = "ws_raw"
      "--TARGET_DB"        = "ws_curated"
      "--DRY_RUN"          = "false"
      "--SOURCE_PATH"      = "s3a://${aws_s3_bucket.bucket.bucket}/raw"
      "--WAREHOUSE_DIR"    = "s3a://${aws_s3_bucket.bucket.bucket}/tables"
      "--extra-py-files"   = "s3://${aws_s3_bucket.bucket.bucket}/jobs/libs/config.py,s3://${aws_s3_bucket.bucket.bucket}/jobs/libs/main.py"
    }

  execution_property {
    max_concurrent_runs = 1
  }
}


#=====================================
# Step functions state machine: ws-workflow
#=====================================

data "aws_iam_policy_document" "stepfunctions_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "stepfunctions_role" {
  name               = "ws-stepfunctions-role"
  assume_role_policy = data.aws_iam_policy_document.stepfunctions_policy.json
}

locals {
  stepfunctions_permissions = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"
  ]
}

resource "aws_iam_role_policy_attachment" "stepfunctions_role_attachment" {
  for_each   = toset(local.stepfunctions_permissions)
  role       = aws_iam_role.stepfunctions_role.name
  policy_arn = each.value
}


resource "aws_cloudwatch_log_group" "ws_workflow" {
  name              = "/aws/vendedlogs/states/ws-workflow"
}

resource "aws_sfn_state_machine" "ws_workflow" {
  name     = "ws-workflow"
  role_arn = aws_iam_role.stepfunctions_role.arn
  definition = <<EOF
  {
    "Comment": "ws-ingest lambda -> ws-glue job",
    "StartAt": "IngestLambda",
    "States": {
      "IngestLambda": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "OutputPath": "$.Payload",
        "Parameters": {
          "FunctionName": "wistia_to_s3",
          "Payload": {
            "pipeline_mode": "incremental",
            "persist_state": true
          }
        },
        "Next": "GlueJob"
      },
      "GlueJob": {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": {
          "JobName": "ws-raw-to-curated",
            "Arguments": {
            "--PIPELINE_MODE": "incremental",
            "--START_DATE.$": "$.start_date",
            "--DRY_RUN": "false"
          }
        },
        "End": true
      }
    },
    "TimeoutSeconds": 900
  }
  EOF

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.ws_workflow.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

#=====================================
# EventBridge Scheduler to trigger Step Functions daily
#=====================================

resource "aws_iam_role" "eventbridge_scheduler_role" {
  name = "ws-eventbridge-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_scheduler_policy" {
  name = "ws-eventbridge-scheduler-policy"
  role = aws_iam_role.eventbridge_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "states:StartExecution"
        Resource = aws_sfn_state_machine.ws_workflow.arn
      }
    ]
  })
}

resource "aws_scheduler_schedule" "ws_workflow_daily" {
  name       = "ws-workflow-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 0 * * ? *)" # every day at 00:00 UTC
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.ws_workflow.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn
  }
}
