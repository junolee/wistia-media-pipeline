#!/bin/sh
set -a
. ./.env
set +a

JOB_NAME="wistia_job"
PIPELINE_MODE="incremental"
START_DATE="2026-01-16"
TARGET_DB=$TARGET_DB # $TARGET_TEST_DB

SCRIPT_FILE_NAME="run.py"
EXTRA_CONFIG_FILE="config.py"
EXTRA_MAIN_FILE="main.py"

CONTAINER_HOME="/home/hadoop"
CONTAINER_WORKSPACE="$CONTAINER_HOME/workspace"

docker run -it --rm \
  -v "$HOME/.aws:$CONTAINER_HOME/.aws" \
  -v "$HOST_WORKSPACE:$CONTAINER_WORKSPACE" \
  -e "AWS_PROFILE=$AWS_PROFILE_NAME" \
  --name glue5_spark_submit \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit "$CONTAINER_WORKSPACE/$SCRIPT_FILE_NAME" \
    --extra-py-files $CONTAINER_WORKSPACE/$EXTRA_CONFIG_FILE,$CONTAINER_WORKSPACE/$EXTRA_MAIN_FILE \
    --JOB_NAME $JOB_NAME \
    --SOURCE_DB $SOURCE_DB \
    --SOURCE_PATH $SOURCE_PATH \
    --WAREHOUSE_DIR $WAREHOUSE_DIR \
    --TARGET_DB $TARGET_DB \
    --PIPELINE_MODE $PIPELINE_MODE \
    --START_DATE $START_DATE
