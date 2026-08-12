#!/usr/bin/env bash
# Orchestrate the caching-benchmark scenarios from the doc.
#
# Runs, in order:
#   1. First replica         - cold (page cache dropped)
#   2. Nth replica           - warm (page cache populated by run 1)
#   3. Nth replica, clean    - page cache dropped (served from the NVME cache, not
#                              page cache); only meaningful when a cache proxy is used
#   4. 2 replicas at once    - under network measurement (RX/TX)
#
# Runs one arm of the Direct-vs-Dragonfly experiment. All extra args pass through to
# model_streamer_stream.py. For the Dragonfly arm, start the proxy and pass its env:
#
#   INTERFACE=ens5 ./run_scenarios.sh \
#       --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com
#
# For the Direct arm (no cache), pass no proxy args:
#
#   INTERFACE=ens5 ./run_scenarios.sh
#
# To run both arms back to back, use run_experiment.sh.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${PYTHON:-python3}"
STREAM="$HERE/model_streamer_stream.py"

banner() {
    echo
    echo "############################################################"
    echo "# $1"
    echo "############################################################"
}

banner "Scenario 1: First replica (cold, page cache dropped)"
"$PY" "$STREAM" "$@" --drop-page-cache

banner "Scenario 2: Nth replica (warm page cache)"
"$PY" "$STREAM" "$@"

banner "Scenario 3: Nth replica (clean page cache)"
"$PY" "$STREAM" "$@" --drop-page-cache

banner "Scenario 4: 2 replicas at once (with network measurement)"
REPLICAS=2 "$HERE/measure_network.sh" "$HERE/same_time.sh" "$@"
