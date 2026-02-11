#!/bin/sh
set -e

S3_BUCKET=jl-wistia-pipeline
LAMBDA_NAME=$1

if [ -z "$LAMBDA_NAME" ]; then
  echo "Usage: $0 <lambda_name>"
  exit 1
fi

ZIP_NAME="${LAMBDA_NAME}.zip"

BUILD_DIR=build_lambda

rm -rf "$BUILD_DIR" "$ZIP_NAME"
mkdir -p "$BUILD_DIR"

cp *.py "$BUILD_DIR"/
rm -f "$BUILD_DIR/local_run.py"

pip install -r requirements-lambda.txt -t "$BUILD_DIR"

find "$BUILD_DIR" -type d -name "__pycache__" -exec rm -rf {} +
find "$BUILD_DIR" -type f -name "*.pyc" -delete

cd "$BUILD_DIR"
zip -r "../$ZIP_NAME" .
cd ..

aws s3 cp "$ZIP_NAME" "s3://$S3_BUCKET/lambda/$ZIP_NAME"

rm -rf "$BUILD_DIR" "$ZIP_NAME"

aws lambda update-function-code --function-name "$LAMBDA_NAME" --s3-bucket "$S3_BUCKET" --s3-key "lambda/$ZIP_NAME" --publish
