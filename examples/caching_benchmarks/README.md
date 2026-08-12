# Run:ai Model Streamer — Caching Benchmarks

Reproduction kit for the **RunAI Model Streamer Caching Benchmarks** doc. It measures
model-load time and throughput when streaming safetensors from S3, comparing **no
cache** against an on-node caching proxy — **S3 Hybrid Cache** or **Dragonfly** —
running in `proxy_only` mode.

The streamer code path is identical in every case; only the environment changes.
The cache is a local **HTTP forward proxy**: the client streams from the real S3
endpoint over HTTP but through `http://127.0.0.1:3128`, so the proxy can cache byte
ranges on NVME. Which cache you get (S3 Hybrid vs Dragonfly) is simply whichever
binary is listening on the proxy port — it is not a client setting.

## Setup (from the doc)

- **Run:ai Model Streamer**: 0.16.1
- **Machine**: AWS `g5.12xlarge` (4 × A10)
  - `gp3`: 2000 MiB/s, 32000 IOPS
  - NVMe: 2.537 GiB/s
- **Model**: Falcon-40b (78.4 GiB), 9 shards under `s3://core-llm/falcon-40b`
- **Cache builds**:
  - S3 Hybrid Cache: `30d886c4056e9e49039f754f39abada152a0845d` (cache dir on NVMe)
  - Dragonfly: `333c7f139855eddd2ff752e32a899814e0bab607` (cache dir on NVMe)

## Files

| File | Purpose |
| --- | --- |
| `model_streamer_stream.py` | Stream the shards and print time + throughput (the doc's `model-streamer-only.py`, parametrized) |
| `same_time.sh` | Run N replicas (default 2) in parallel on one node |
| `measure_network.sh` | Wrap a command and report RX/TX from `/sys/class/net/$INTERFACE` |
| `run_scenarios.sh` | Orchestrate all scenarios (cold / warm / clean page cache / 2 replicas) |
| `s3_proxy_config.yaml` | Proxy config (`proxy_only`, port 3128, NVMe cache dir) |

## Running

### Without cache

```bash
INTERFACE=ens5 ./run_scenarios.sh
```

### With cache

Start the proxy you want to benchmark (S3 Hybrid Cache or Dragonfly) in `proxy_only`
mode, then pass the cache env:

```bash
# start the proxy (example: S3 Hybrid Cache binary)
./target/release/s3-proxy -c s3_proxy_config.yaml

# run the benchmarks through it
INTERFACE=ens5 ./run_scenarios.sh \
    --proxy http://127.0.0.1:3128 \
    --endpoint http://s3.us-east-1.amazonaws.com
```

### Single stream, by hand

```bash
# no cache
python model_streamer_stream.py

# with cache
python model_streamer_stream.py \
    --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com

# clean-page-cache scenario (needs sudo)
python model_streamer_stream.py --drop-page-cache \
    --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com
```

Clean the page cache manually with:

```bash
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

## Scenarios

**Single replica sequential streaming**
- *First replica* — cold; with a cache this is bottlenecked by the write to NVMe.
- *Nth replica (PageCache)* — page cache warm; fastest.
- *Nth replica (Clean PageCache)* — page cache dropped; served from the NVMe cache,
  so bottlenecked by NVMe.

**2 replicas at once** — two streams in parallel under `measure_network.sh`. Without a
cache, RX ≈ 2× the model size (each replica fetches independently); with a cache RX ≈
1× (the second replica is served from cache).

## Results (from the doc)

### S3 caching — single replica

Model size 78.4 GiB. Throughput is GiB/s.

| | Without cache — First | Without cache — Second | S3 Hybrid — First | S3 Hybrid — Nth (PageCache) | S3 Hybrid — Nth (Clean) | Dragonfly — First | Dragonfly — Nth (PageCache) | Dragonfly — Nth (Clean) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Seconds | 19.155 | 18.803 | 39.101 | 12.736 | 41.684 | 254.482 | 15.020 | 41.731 |
| Throughput | 4.066 | 4.142 | 2.005 | 6.156 | 1.880 | — | — | — |

Key points:
1. Without a cache the two reads are the same.
2. With a cache the first read is slower (bottlenecked by the write to NVMe).
3. The warm (page cache) read benefits from page cache — partially; a pure page-cache
   read would be faster still.
4. Without page cache, performance is bottlenecked by NVMe.

### S3 caching — 2 replicas at once

| | Without cache | With cache (S3 Hybrid) | With cache (Dragonfly) |
| --- | --- | --- | --- |
| Time total for both replicas (s) | 35.288 | — | 71.419 |
| Time per replica (s) | 35.271 / 35.288 | 44.962 | 71.419 |
| RX (GiB) | 157.36 | 82.57 | |
| TX (GiB) | 0.51 | 0.15 | |
| Total (GiB) | 157.87 | 82.72 | |

Key points:
1. Without a cache: ~2× the single-replica time, each replica ~0.5× throughput, and
   RX ≈ 2× the model size.
2. With a cache: bottlenecked by NVMe, not necessarily balanced between replicas, and
   RX ≈ 1× the model size.

*(Some cells are transcribed as-is from the doc; blanks are values the doc did not
list.)*

### NFS caching (context)

The doc also covers NFS caching via `cachefilesd` (FS-Cache). That path streams from an
NFS mount rather than an S3 proxy, so it is not driven by the scripts here, but the
streaming step is the same `SafetensorsStreamer` loop. Summary from the doc:

- Single replica, no cache: 158.753 s (0.469 GiB/s), saturating NFS; the second
  (page-cache) replica: 2.526 s (29.49 GiB/s).
- With NVMe cache: first replica 160.888 s; Nth warm 2.541 s (29.31 GiB/s); Nth clean
  page cache 46.041 s (1.619 GiB/s, NVMe-bound).

## Appendix: the `range(0,0)` primer

Not part of the doc baseline, so it is off by default. Some Dragonfly proxy setups, on
a cold ranged GET, synthesize their own `Range: bytes=0-0` probe toward the origin to
learn the object size. That synthesized probe reuses the caller's SigV4 authorization
but mutates the signed `Range` header, so S3 rejects it (`SignatureDoesNotMatch`).

`--prime-range0` issues an explicit, correctly-signed `Range: bytes=0-0` GET per object
first, through the SDK (a 1-byte range at offset 0), so the proxy caches object
metadata from a valid request and never has to forge its own broken probe:

```bash
python model_streamer_stream.py --prime-range0 \
    --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com
```
