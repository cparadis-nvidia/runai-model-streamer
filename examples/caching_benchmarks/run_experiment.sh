#!/usr/bin/env bash
# Direct (no cache) vs Dragonfly — the experiment.
#
# Runs the full scenario set twice: once streaming straight from S3 (direct, no
# cache) and once through the Dragonfly proxy. Start the Dragonfly proxy on the
# proxy port first (see README), then:
#
#   INTERFACE=ens5 ./run_experiment.sh
#
# Overridable via env:
#   DRAGONFLY_PROXY  proxy URL          (default http://127.0.0.1:3128)
#   ENDPOINT         AWS_ENDPOINT_URL   (default http://s3.us-east-1.amazonaws.com)
#   INTERFACE        NIC for RX/TX      (default ens5)
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRAGONFLY_PROXY="${DRAGONFLY_PROXY:-http://127.0.0.1:3128}"
ENDPOINT="${ENDPOINT:-http://s3.us-east-1.amazonaws.com}"

echo "############################################################"
echo "# ARM 1: DIRECT (no cache)"
echo "############################################################"
"$HERE/run_scenarios.sh"

echo
echo "############################################################"
echo "# ARM 2: DRAGONFLY (proxy $DRAGONFLY_PROXY)"
echo "############################################################"
"$HERE/run_scenarios.sh" --proxy "$DRAGONFLY_PROXY" --endpoint "$ENDPOINT"
