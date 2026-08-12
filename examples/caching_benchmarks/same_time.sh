#!/usr/bin/env bash
# Run N replicas of the streaming script at once on the same node.
#
# Reproduces same_time.sh from the caching-benchmarks doc (default 2 replicas).
# Any extra args are passed through to model_streamer_stream.py, e.g.:
#
#   ./same_time.sh --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com
#   REPLICAS=4 ./same_time.sh
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${PYTHON:-python3}"
SCRIPT="$HERE/model_streamer_stream.py"
REPLICAS="${REPLICAS:-2}"

echo "Launching $REPLICAS replica(s) of $SCRIPT"
pids=()
for _ in $(seq 1 "$REPLICAS"); do
    "$PY" "$SCRIPT" "$@" &
    pids+=("$!")
done

rc=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        rc=1
    fi
done
exit "$rc"
