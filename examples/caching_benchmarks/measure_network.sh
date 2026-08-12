#!/usr/bin/env bash
# Measure RX/TX network traffic over the lifetime of a command.
#
# Reproduces measure_network.sh from the caching-benchmarks doc, used to show that
# received traffic (RX) is ~1x the model size with a cache vs ~2x for two uncached
# replicas. Reads counters from /sys/class/net/$INTERFACE/statistics.
#
#   INTERFACE=ens5 ./measure_network.sh ./same_time.sh --proxy http://127.0.0.1:3128
set -u

INTERFACE="${INTERFACE:-ens5}"

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <command> [args...]"
    exit 1
fi

RX_BEFORE=$(cat "/sys/class/net/$INTERFACE/statistics/rx_bytes")
TX_BEFORE=$(cat "/sys/class/net/$INTERFACE/statistics/tx_bytes")
START=$(date +%s)

echo "============================================================"
echo " Network measurement"
echo "============================================================"
echo "Interface: $INTERFACE"
echo "Command: $*"
echo
echo "RX before: $RX_BEFORE bytes"
echo "TX before: $TX_BEFORE bytes"
echo

"$@"
EXIT_CODE=$?

END=$(date +%s)
DURATION=$((END - START))
RX_AFTER=$(cat "/sys/class/net/$INTERFACE/statistics/rx_bytes")
TX_AFTER=$(cat "/sys/class/net/$INTERFACE/statistics/tx_bytes")
RX=$((RX_AFTER - RX_BEFORE))
TX=$((TX_AFTER - TX_BEFORE))
TOTAL=$((RX + TX))

python3 - "$RX" "$TX" "$TOTAL" "$DURATION" <<'PY'
import sys

rx, tx, total, duration = map(int, sys.argv[1:])

def human(n):
    if n >= 1024**4:
        return f"{n / 1024**4:.2f} TiB"
    if n >= 1024**3:
        return f"{n / 1024**3:.2f} GiB"
    if n >= 1024**2:
        return f"{n / 1024**2:.2f} MiB"
    if n >= 1024:
        return f"{n / 1024:.2f} KiB"
    return f"{n} B"

print()
print("============================================================")
print(" Network usage")
print("============================================================")
print(f"RX:\t{human(rx)}")
print(f"TX:\t{human(tx)}")
print(f"TOTAL:\t{human(total)}")
print(f"Duration: {duration}s")
if duration:
    print(f"RX rate:\t{human(rx / duration)}/s")
    print(f"TX rate:\t{human(tx / duration)}/s")
    print(f"Total rate: {human(total / duration)}/s")
print("============================================================")
PY

echo "Exit code: $EXIT_CODE"
exit "$EXIT_CODE"
