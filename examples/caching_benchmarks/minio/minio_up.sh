#!/usr/bin/env bash
# Start a local MinIO server (S3-compatible) in Docker for the benchmark.
#
# API on :9000, web console on :9001. Credentials default to minioadmin/minioadmin.
# Data is persisted in a named volume so a restart keeps the seeded model.
#
#   ./minio_up.sh            # start
#   ./minio_up.sh stop       # stop + remove the container (keeps the volume)
set -eu

NAME="${MINIO_NAME:-minio-benchmark}"
API_PORT="${MINIO_API_PORT:-9000}"
CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
VOLUME="${MINIO_VOLUME:-minio-benchmark-data}"

if [[ "${1:-}" == "stop" ]]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    echo "Stopped and removed container '$NAME' (volume '$VOLUME' kept)."
    exit 0
fi

if docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
    echo "MinIO container '$NAME' is already running."
    exit 0
fi

docker rm -f "$NAME" >/dev/null 2>&1 || true

docker run -d \
    --name "$NAME" \
    -p "$API_PORT:9000" \
    -p "$CONSOLE_PORT:9001" \
    -e "MINIO_ROOT_USER=$ROOT_USER" \
    -e "MINIO_ROOT_PASSWORD=$ROOT_PASSWORD" \
    -v "$VOLUME:/data" \
    minio/minio server /data --console-address ":9001" >/dev/null

echo "Waiting for MinIO to become ready on :$API_PORT ..."
for _ in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:$API_PORT/minio/health/ready" >/dev/null 2>&1; then
        echo "MinIO is ready."
        echo "  API:     http://127.0.0.1:$API_PORT"
        echo "  Console: http://127.0.0.1:$CONSOLE_PORT  (user: $ROOT_USER)"
        exit 0
    fi
    sleep 1
done

echo "MinIO did not become ready in time; check 'docker logs $NAME'." >&2
exit 1
