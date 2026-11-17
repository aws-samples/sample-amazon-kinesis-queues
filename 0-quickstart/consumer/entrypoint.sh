#!/bin/sh
set -eu

: "${STREAM_NAME:?Set STREAM_NAME to the Kinesis stream name.}"
: "${AWS_REGION:?Set AWS_REGION to the AWS Region.}"
: "${APPLICATION_NAME:?Set APPLICATION_NAME to the KCL application name.}"

exec java -jar /app/consumer.jar \
    --stream-name "$STREAM_NAME" \
    --region "$AWS_REGION" \
    --application-name "$APPLICATION_NAME"
