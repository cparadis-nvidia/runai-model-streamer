# Model Streamer + Hybrid Cache for S3 demo benchmark

This benchmark produces a controlled comparison of the same model and Model
Streamer configuration in three scenarios:

1. `direct_s3` — direct HTTPS reads from Amazon S3, without the proxy.
2. `hybrid_cache_fill` — the first read through an empty Hybrid Cache instance.
3. `hybrid_cache_warm` — repeated reads served by the populated cache.

The requested **WITHOUT versus WITH** result is the median of `direct_s3`
versus `hybrid_cache_warm`. The fill result is retained because it makes proxy
overhead and cache population visible instead of hiding them.

The runner starts a fresh, loopback-only proxy for every benchmark, alternates
the direct and warm-cache measured runs, uses a new Python process for every
sample, and verifies the cached arm using the proxy's `bytes_saved` metric.

## Recommended demo environment

- Linux host or EC2 instance with enough RAM for the selected device and enough
  local SSD space for at least the model size plus 10% headroom.
- Bucket and host placement chosen for the story being demonstrated. A
  cross-region or bandwidth-constrained origin demonstrates the cache value
  more clearly than same-region S3 for a single client.
- One immutable safetensors model. Use exact object URIs rather than a mutable
  prefix so every arm reads identical bytes.
- AWS credentials with `s3:GetObject` for every shard.

Use CPU for a storage-only demo that works without a GPU. Use `cuda:0` to show
the actual storage-to-GPU Model Streamer path; keep the same device for all arms.

## Build the two projects

Build and install this checkout so the native S3 client includes the opt-in
system-proxy support used by the cached arm:

```bash
cd /Users/cparadis/code/runai-model-streamer
make build
python3 -m pip install --force-reinstall \
  py/runai_model_streamer/dist/*.whl \
  py/runai_model_streamer_s3/dist/*.whl
```

Clone and build the AWS sample on the benchmark host:

```bash
git clone https://github.com/aws-samples/sample-s3-hybrid-cache.git
cd sample-s3-hybrid-cache
cargo build --release
```

The Hybrid Cache project is AWS sample code for demonstration and education;
review its security guidance before using it outside an isolated demo.

## Define the model

Copy `model-uris.example.txt` and put one exact S3 URI per line:

```text
s3://demo-bucket/llama/model-00001-of-00004.safetensors
s3://demo-bucket/llama/model-00002-of-00004.safetensors
s3://demo-bucket/llama/model-00003-of-00004.safetensors
s3://demo-bucket/llama/model-00004-of-00004.safetensors
```

## Run the demo

```bash
cd /Users/cparadis/code/runai-model-streamer
python3 benchmarks/s3_hybrid_cache/benchmark.py \
  --manifest benchmarks/s3_hybrid_cache/model-uris.txt \
  --region us-east-1 \
  --proxy-binary /path/to/sample-s3-hybrid-cache/target/release/s3-proxy \
  --device cpu \
  --concurrency 32 \
  --direct-runs 3 \
  --warm-runs 3 \
  --cache-size-gib 32
```

For a GPU run, replace `--device cpu` with `--device cuda:0`. Set
`--cache-size-gib` above the total model size while keeping it at or below 90%
of available cache storage.

## Artifacts and interpretation

Each run creates a timestamped directory under `results/` containing:

- `report.json` — configuration, individual samples, summary, and cache metric deltas.
- `samples.csv` — demo-friendly timing and throughput rows.
- `hybrid-cache.yaml` — the exact generated proxy configuration.
- `hybrid-cache-process.log` and `proxy-logs/` — routing evidence and diagnostics.
- `samples/*.log` — Model Streamer output for each isolated run.

The command exits with status 2 when warm runs report zero `bytes_saved`. That
protects the demo from presenting a timing comparison when traffic bypassed the
cache.

Report at least these values:

| Scenario | Median load time | Mean throughput | Cache evidence |
|---|---:|---:|---:|
| Direct S3 | `direct_s3` | `direct_s3` | Proxy counters unchanged |
| Hybrid Cache, cold | `hybrid_cache_fill` | `hybrid_cache_fill` | Misses and cache growth |
| Hybrid Cache, warm | `hybrid_cache_warm` | `hybrid_cache_warm` | `bytes_saved > 0` |

Do not describe the cold fill as an acceleration result: it includes S3 fetch,
proxy processing, and cache writes. The warm result is the repeated-load result
the cache is designed to improve.

## Security boundary

The generated proxy listens only on loopback, and the client-to-proxy hop is
plain HTTP so the cache can process the request. SigV4 authenticates the request
but does not encrypt that hop. The generated one-hour cache TTL also means warm
hits can be served without a new S3 authorization check. Do not expose this demo
proxy to another host or untrusted network.

References:

- [Hybrid Cache for Amazon S3](https://github.com/aws-samples/sample-s3-hybrid-cache)
- [AWS SDK for C++ proxy configuration](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html)
