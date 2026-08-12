#!/usr/bin/env bash
# Direct (no cache) vs Dragonfly — the experiment.
#
# Runs the full scenario set twice: once streaming straight from the origin
# (direct, no cache) and once through the Dragonfly proxy. Any args you pass are
# COMMON to both arms (e.g. the origin --endpoint, --path-style, credentials); the
# Dragonfly arm additionally gets --proxy.
#
#   # Real S3:
#   INTERFACE=ens5 ./run_experiment.sh --endpoint http://s3.us-east-1.amazonaws.com
#
#   # MinIO origin:
#   INTERFACE=ens5 ./run_experiment.sh \
#       --endpoint http://127.0.0.1:9000 --path-style \
#       --access-key minioadmin --secret-key minioadmin --region us-east-1
#
# Start the Dragonfly proxy on the proxy port first. Overridable via env:
#   DRAGONFLY_PROXY  proxy URL     (default http://127.0.0.1:3128)
#   INTERFACE        NIC for RX/TX (default ens5)
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRAGONFLY_PROXY="${DRAGONFLY_PROXY:-http://127.0.0.1:3128}"
COMMON=("$@")

echo "############################################################"
echo "# ARM 1: DIRECT (no cache)"
echo "############################################################"
"$HERE/run_scenarios.sh" "${COMMON[@]}"

echo
echo "############################################################"
echo "# ARM 2: DRAGONFLY (proxy $DRAGONFLY_PROXY)"
echo "############################################################"
"$HERE/run_scenarios.sh" "${COMMON[@]}" --proxy "$DRAGONFLY_PROXY"
