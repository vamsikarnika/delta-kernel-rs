#!/usr/bin/env bash
# Runs the ignored cloud tests (tests/cloud.rs) against local emulators in Docker:
#   S3    adobe/s3mock                     (MinIO no longer publishes images)
#   GCS   tustvold/fake-gcs-server         (the fork object_store's own CI uses; the stock
#                                           fsouza image does not serve object_store's URLs)
#   Azure mcr.microsoft.com/azure-storage/azurite
# Needs Docker, python3 and a working `cargo`. Containers are removed on exit.
set -euo pipefail
cd "$(dirname "$0")/.."

cleanup() { docker rm -f delta-cpp-s3 delta-cpp-gcs delta-cpp-azurite >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

wait_for() { # url, expected-not-000
  for _ in $(seq 1 30); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$1" || true)
    [ "$code" != "000" ] && return 0
    sleep 2
  done
  echo "timed out waiting for $1" >&2; return 1
}

docker run -d --name delta-cpp-s3 -p 127.0.0.1:9090:9090 \
  -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=delta-test,delta-test-2 adobe/s3mock >/dev/null
# `localhost`, not 127.0.0.1: the emulator routes object_store's path-style URLs by its public host.
docker run -d --name delta-cpp-gcs -p 127.0.0.1:4443:4443 tustvold/fake-gcs-server \
  -scheme http -backend memory -public-host localhost:4443 >/dev/null
docker run -d --name delta-cpp-azurite -p 127.0.0.1:10000:10000 \
  mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck >/dev/null

wait_for http://127.0.0.1:9090/
wait_for http://127.0.0.1:4443/storage/v1/b
wait_for http://127.0.0.1:10000/devstoreaccount1
for b in delta-test delta-test-2; do
  curl -sf -X POST --data-binary "{\"name\":\"$b\"}" -H 'Content-Type: application/json' \
    http://127.0.0.1:4443/storage/v1/b >/dev/null
done
python3 -I tests/create_azurite_containers.py delta-test delta-test-2

DELTA_CPP_TEST_S3_ENDPOINT=http://127.0.0.1:9090 \
DELTA_CPP_TEST_GCS_ENDPOINT=http://localhost:4443 \
DELTA_CPP_TEST_AZURE=1 \
  cargo test -p delta-cpp --test cloud -- --ignored "$@"
