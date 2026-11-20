#!/bin/sh
# Create the warehouse bucket. Idempotent: safe to run on every `make up`.
set -e
EP="--endpoint-url http://s3:9000"
for i in $(seq 1 60); do
  aws $EP s3api list-buckets >/dev/null 2>&1 && break
  sleep 1
done
for b in warehouse; do
  aws $EP s3api head-bucket --bucket "$b" >/dev/null 2>&1 || aws $EP s3api create-bucket --bucket "$b" >/dev/null
  echo "bucket ready: $b"
done
