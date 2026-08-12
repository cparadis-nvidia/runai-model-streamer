# Run:ai Model Streamer — Direct vs Dragonfly Caching Benchmark

Reproduction kit for the caching experiment: **Direct (no cache) vs Dragonfly**.
It measures model-load time and throughput when streaming safetensors from S3,
comparing streaming **straight from S3** against streaming through an on-node
**Dragonfly** caching proxy running in `proxy_only` mode.

> The source doc also reports an S3 Hybrid Cache arm. That is **reference only** —
> our experiment is Direct vs Dragonfly. The S3 Hybrid numbers are kept at the
> bottom for context.

The streamer code path is identical in both arms; only the environment changes.
The cache is a local **HTTP forward proxy**: the client streams from the real S3
endpoint over HTTP but through `http://127.0.0.1:3128`, so Dragonfly can cache byte
ranges on NVMe.

## Setup (from the doc)

- **Run:ai Model Streamer**: 0.16.1
- **Machine**: AWS `g5.12xlarge` (4 × A10)
  - `gp3`: 2000 MiB/s, 32000 IOPS
  - NVMe: 2.537 GiB/s
- **Model**: Falcon-40b (78.4 GiB), 9 shards under `s3://core-llm/falcon-40b`
- **Dragonfly build**: `333c7f139855eddd2ff752e32a899814e0bab607` (cache dir on NVMe)

## Files

| File | Purpose |
| --- | --- |
| `model_streamer_stream.py` | Stream the shards and print time + throughput (the doc's `model-streamer-only.py`, parametrized) |
| `run_experiment.sh` | Run the whole experiment: Direct arm, then Dragonfly arm |
| `run_scenarios.sh` | One arm's scenarios (cold / warm / clean page cache / 2 replicas) |
| `same_time.sh` | Run N replicas (default 2) in parallel on one node |
| `measure_network.sh` | Wrap a command and report RX/TX from `/sys/class/net/$INTERFACE` |
| `s3_proxy_config.yaml` | Example `proxy_only` config (see note below) |
| `minio/minio_up.sh` | Start a local MinIO (S3-compatible) server in Docker |
| `minio/seed_minio.py` | Generate synthetic safetensors shards and upload them to MinIO |

## Quickstart with MinIO (no real S3 needed)

If you don't have permissions for a real S3 bucket, run the experiment against a
local **MinIO** server. MinIO is S3-compatible, so the streamer path is unchanged —
you just point at the MinIO endpoint and use **path-style** addressing.

```bash
# 1. start MinIO (API :9000, console :9001, minioadmin/minioadmin)
./minio/minio_up.sh

# 2. seed synthetic shards (real safetensors; scale --shard-mib / --num-shards)
python minio/seed_minio.py \
    --endpoint http://127.0.0.1:9000 \
    --bucket core-llm --prefix falcon-40b \
    --num-shards 9 --shard-mib 256

# 3a. single stream, direct (no cache)
python model_streamer_stream.py \
    --root s3://core-llm/falcon-40b --num-shards 9 \
    --endpoint http://127.0.0.1:9000 --path-style \
    --access-key minioadmin --secret-key minioadmin --region us-east-1

# 3b. whole experiment, Direct vs Dragonfly (start the Dragonfly proxy first,
#     pointing its origin at the MinIO endpoint)
INTERFACE=lo ./run_experiment.sh \
    --root s3://core-llm/falcon-40b --num-shards 9 \
    --endpoint http://127.0.0.1:9000 --path-style \
    --access-key minioadmin --secret-key minioadmin --region us-east-1
```

Notes for MinIO:
- The seeded shards are **synthetic**, not the real Falcon-40b weights — use them to
  exercise the Direct-vs-Dragonfly path, then scale the size toward the real model.
- Path-style addressing is required (`--path-style`), and MinIO accepts any region.
- For a local single-node MinIO the meaningful network interface is often `lo`.

## Running

### The experiment (both arms)

Start the Dragonfly proxy on the proxy port, then:

```bash
INTERFACE=ens5 ./run_experiment.sh
```

This runs the Direct arm (no cache) followed by the Dragonfly arm
(`--proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com`).

### One arm at a time

```bash
# Direct (no cache)
INTERFACE=ens5 ./run_scenarios.sh

# Dragonfly
INTERFACE=ens5 ./run_scenarios.sh \
    --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com
```

### Single stream, by hand

```bash
# Direct
python model_streamer_stream.py

# Dragonfly
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

> **Proxy config note:** `s3_proxy_config.yaml` is the example `proxy_only` config
> from the doc (it was used for the S3 Hybrid `s3-proxy` binary). For the Dragonfly
> arm, launch the Dragonfly proxy per its own docs on the same proxy port (3128);
> the client side is unchanged.

## Scenarios

**Single replica sequential streaming**
- *First replica* — cold; with Dragonfly this is bottlenecked by the write to NVMe.
- *Nth replica (PageCache)* — page cache warm; fastest.
- *Nth replica (Clean PageCache)* — page cache dropped; served from the NVMe cache,
  so bottlenecked by NVMe. (Direct has no NVMe cache, so this is just another cold
  S3 read.)

**2 replicas at once** — two streams in parallel under `measure_network.sh`. Direct:
RX ≈ 2× the model size (each replica fetches independently). Dragonfly: RX ≈ 1×
(the second replica is served from cache).

## Results (from the doc)

Model size 78.4 GiB. Throughput is GiB/s.

### Direct vs Dragonfly — single replica

| | Direct — First | Direct — Second | Dragonfly — First | Dragonfly — Nth (PageCache) | Dragonfly — Nth (Clean) |
| --- | --- | --- | --- | --- | --- |
| Seconds | 19.155 | 18.803 | 254.482 | 15.020 | 41.731 |
| Throughput | 4.066 | 4.142 | 0.308 | 5.220 | 1.879 |

Key points:
1. Direct (no cache): the two reads are the same (~4 GiB/s).
2. Dragonfly first read is much slower — bottlenecked by the cache-fill write to NVMe.
3. Dragonfly warm (page cache) read is the fastest (~5.2 GiB/s).
4. Dragonfly without page cache is bottlenecked by NVMe (~1.9 GiB/s).

*(Dragonfly throughput derived from the doc's seconds and the 78.4 GiB model size.)*

### Direct vs Dragonfly — 2 replicas at once

| | Direct (no cache) | Dragonfly |
| --- | --- | --- |
| Time total for both replicas (s) | 35.288 | 71.419 |
| Time per replica (s) | 35.271 / 35.288 | 71.419 |
| RX (GiB) | 157.36 | 82.57 |
| TX (GiB) | 0.51 | 0.15 |
| Total (GiB) | 157.87 | 82.72 |

Key points:
1. Direct: ~2× the single-replica time, each replica ~0.5× throughput, RX ≈ 2× the
   model size.
2. Dragonfly: bottlenecked by NVMe, not necessarily balanced between replicas, RX ≈
   1× the model size.

*(Some cells are transcribed as-is from the doc.)*

### Reference only — S3 Hybrid Cache (not our experiment)

Kept for context; not part of the Direct-vs-Dragonfly comparison.

| | S3 Hybrid — First | S3 Hybrid — Nth (PageCache) | S3 Hybrid — Nth (Clean) |
| --- | --- | --- | --- |
| Seconds | 39.101 | 12.736 | 41.684 |
| Throughput | 2.005 | 6.156 | 1.880 |

## Appendix: the `range(0,0)` primer

Not part of the doc baseline, so it is off by default. Some Dragonfly proxy setups, on
a cold ranged GET, synthesize their own `Range: bytes=0-0` probe toward the origin to
learn the object size. That synthesized probe reuses the caller's SigV4 authorization
but mutates the signed `Range` header, so S3 rejects it (`SignatureDoesNotMatch`).

`--prime-range0` issues an explicit, correctly-signed `Range: bytes=0-0` GET per object
first, through the SDK (a 1-byte range at offset 0), so Dragonfly caches object
metadata from a valid request and never has to forge its own broken probe:

```bash
python model_streamer_stream.py --prime-range0 \
    --proxy http://127.0.0.1:3128 --endpoint http://s3.us-east-1.amazonaws.com
```
