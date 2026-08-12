#!/usr/bin/env python3
"""Benchmark: direct S3 read vs DragonFly + S3 read using the Run:ai Model Streamer.

This script streams every ``*.safetensors`` object under an S3 prefix twice and
compares throughput:

  * ``direct``    - the streamer talks straight to S3 (or your origin endpoint).
  * ``dragonfly`` - the streamer talks to a DragonFly proxy that fronts the same
                    S3 bucket.

Both paths use the exact same Run:ai Model Streamer code path (ranged GETs issued
by the C++ AWS CRT client); only the endpoint the streamer points at changes.

The DragonFly "range(0,0)" primer
---------------------------------
The streamer reads objects with HTTP ``Range`` requests. When those ranged GETs
hit DragonFly *cold* (object not yet known to DragonFly), DragonFly first has to
learn the object size, so it synthesizes its own ``Range: bytes=0-0`` probe toward
the origin. That synthesized probe reuses the caller's SigV4 authorization but with
a mutated ``Range`` header, so the request no longer matches the signature the
client computed -> S3 rejects it (``SignatureDoesNotMatch``) and the read fails.

The fix, per Omer Dayan: issue an explicit, correctly-signed ``Range: bytes=0-0``
GET for each object *ourselves* before the real streamed reads. DragonFly then
learns the object metadata from a valid request and never has to forge its own
broken probe. This priming step is ON by default for the ``dragonfly`` path and
can be turned off with ``--no-prime`` (useful to reproduce the failure).

Usage
-----
    # Compare both, priming DragonFly with range(0,0) first (default):
    python dragonfly_vs_s3_benchmark.py s3://my-bucket/models/llama3-8b/ \
        --dragonfly-endpoint http://dragonfly-proxy:65001 \
        --region us-east-1 --concurrency 32

    # Direct S3 only:
    python dragonfly_vs_s3_benchmark.py s3://my-bucket/models/llama3-8b/ --mode direct

    # Reproduce the broken-signature failure (no primer):
    python dragonfly_vs_s3_benchmark.py s3://my-bucket/models/llama3-8b/ \
        --mode dragonfly --dragonfly-endpoint http://dragonfly-proxy:65001 --no-prime

Credentials are taken from the standard AWS chain (env vars, profile, IMDS, ...).
Only the endpoint (and optionally the region) is overridden per run, so the same
credentials are used for direct and DragonFly reads.

For cold-start accuracy, listing/priming and streaming should be run against a
DragonFly cache in the state you care about; DragonFly caching between iterations
is expected to speed up later iterations.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"expected an s3:// URI, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        raise ValueError(f"missing bucket in {uri!r}")
    return bucket, prefix


def make_boto3_client(endpoint: Optional[str], region: Optional[str], path_style: bool, unsigned: bool):
    """Build a boto3 S3 client for listing / priming.

    Kept independent of the streamer's own boto3 usage so we can point listing at
    the origin and priming at DragonFly with full control over addressing style.
    """
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    config_kwargs = {}
    if path_style:
        config_kwargs["s3"] = {"addressing_style": "path"}
    if unsigned:
        config_kwargs["signature_version"] = UNSIGNED
    config = Config(**config_kwargs) if config_kwargs else None

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        config=config,
    )


def list_safetensors_objects(client, bucket: str, prefix: str) -> List[Tuple[str, int]]:
    """List every ``*.safetensors`` object under ``prefix``; returns (key, size)."""
    paginator = client.get_paginator("list_objects_v2")
    results: List[Tuple[str, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".safetensors"):
                results.append((key, int(obj["Size"])))
    return results


def prime_range_0_0(client, bucket: str, keys: List[str]) -> None:
    """Issue an explicit, signed ``Range: bytes=0-0`` GET for each object.

    Warms DragonFly's object metadata with a valid request so DragonFly never has
    to forge its own range(0,0) probe (which would mutate the signed Range header
    and be rejected by S3). See the module docstring for the full rationale.
    """
    for key in keys:
        resp = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-0")
        body = resp["Body"]
        try:
            body.read()
        finally:
            body.close()


@dataclass
class RunResult:
    label: str
    elapsed_s: float
    bytes_streamed: int

    @property
    def throughput_bps(self) -> float:
        return self.bytes_streamed / self.elapsed_s if self.elapsed_s > 0 else 0.0


def human_bytes(n: float) -> str:
    step = 1024.0
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < step:
            return f"{n:.2f} {unit}"
        n /= step
    return f"{n:.2f} PiB"


def stream_once(paths: List[str], endpoint: Optional[str], region: Optional[str], device: str) -> RunResult:
    """Stream all paths through the Run:ai Model Streamer and time it.

    The endpoint override is what selects direct-S3 vs DragonFly: it is passed as
    ``S3Credentials.endpoint``, which the C++ layer applies as the CRT client's
    endpointOverride. Credentials themselves come from the ambient AWS chain.
    """
    from runai_model_streamer import SafetensorsStreamer
    from runai_model_streamer.s3_utils.s3_utils import S3Credentials

    credentials = S3Credentials(region_name=region, endpoint=endpoint)

    start = time.perf_counter()
    with SafetensorsStreamer() as streamer:
        streamer.stream_files(paths, s3_credentials=credentials, device=device)
        # Consume every tensor to force the full read to complete.
        for _name, _tensor in streamer.get_tensors():
            pass
        bytes_streamed = streamer.total_size
    elapsed = time.perf_counter() - start

    label = "dragonfly" if endpoint else "direct"
    return RunResult(label=label, elapsed_s=elapsed, bytes_streamed=bytes_streamed)


def run_mode(
    label: str,
    stream_endpoint: Optional[str],
    paths: List[str],
    region: Optional[str],
    device: str,
    iterations: int,
    warmup: int,
    prime: bool,
    prime_client,
    bucket: str,
    keys: List[str],
) -> List[RunResult]:
    print(f"\n=== {label} ({len(paths)} file(s), endpoint={stream_endpoint or 'default S3'}) ===")
    results: List[RunResult] = []
    total_iters = warmup + iterations
    for i in range(total_iters):
        is_warmup = i < warmup
        tag = "warmup" if is_warmup else f"iter {i - warmup + 1}/{iterations}"

        if prime:
            print(f"  [{tag}] priming range(0,0) for {len(keys)} object(s) via {label} endpoint ...")
            prime_range_0_0(prime_client, bucket, keys)

        result = stream_once(paths, stream_endpoint, region, device)
        result.label = label
        print(
            f"  [{tag}] streamed {human_bytes(result.bytes_streamed)} in "
            f"{result.elapsed_s:.2f}s -> {human_bytes(result.throughput_bps)}/s"
        )
        if not is_warmup:
            results.append(result)
    return results


def summarize(label: str, results: List[RunResult]) -> Optional[RunResult]:
    if not results:
        return None
    avg_elapsed = sum(r.elapsed_s for r in results) / len(results)
    bytes_streamed = results[0].bytes_streamed
    agg = RunResult(label=label, elapsed_s=avg_elapsed, bytes_streamed=bytes_streamed)
    return agg


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("model_path", help="S3 prefix to stream, e.g. s3://bucket/models/llama3-8b/")
    parser.add_argument(
        "--mode",
        choices=("direct", "dragonfly", "both"),
        default="both",
        help="which path(s) to benchmark (default: both)",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="direct/origin S3 endpoint override (default: real AWS S3). Also used for listing.",
    )
    parser.add_argument(
        "--dragonfly-endpoint",
        default=None,
        help="DragonFly proxy endpoint, e.g. http://dragonfly-proxy:65001 (required for dragonfly/both)",
    )
    parser.add_argument("--region", default=None, help="AWS region for the S3 client")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="RUNAI_STREAMER_CONCURRENCY (number of reader threads)",
    )
    parser.add_argument(
        "--chunk-bytesize",
        type=int,
        default=None,
        help="RUNAI_STREAMER_CHUNK_BYTESIZE (bytes per ranged GET; min 5MiB for object store)",
    )
    parser.add_argument("--iterations", type=int, default=1, help="measured iterations per mode (default: 1)")
    parser.add_argument("--warmup", type=int, default=0, help="unmeasured warmup iterations per mode (default: 0)")
    parser.add_argument("--device", default="cpu", help="destination device (default: cpu)")
    parser.add_argument(
        "--no-prime",
        dest="prime",
        action="store_false",
        help="disable the DragonFly range(0,0) primer (use to reproduce the broken-signature failure)",
    )
    parser.set_defaults(prime=True)
    parser.add_argument(
        "--path-style",
        action="store_true",
        help="use path-style addressing for the boto3 list/prime client (S3-compatible endpoints)",
    )
    parser.add_argument(
        "--unsigned",
        action="store_true",
        help="use anonymous/unsigned requests (public buckets)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    do_direct = args.mode in ("direct", "both")
    do_dragonfly = args.mode in ("dragonfly", "both")

    if do_dragonfly and not args.dragonfly_endpoint:
        print("error: --dragonfly-endpoint is required for mode 'dragonfly' or 'both'", file=sys.stderr)
        return 2

    # Streamer tuning is read from the environment by the C++ core at request time,
    # so set it before any streaming happens.
    if args.concurrency is not None:
        os.environ["RUNAI_STREAMER_CONCURRENCY"] = str(args.concurrency)
    if args.chunk_bytesize is not None:
        os.environ["RUNAI_STREAMER_CHUNK_BYTESIZE"] = str(args.chunk_bytesize)
    if args.unsigned:
        os.environ["RUNAI_STREAMER_S3_UNSIGNED"] = "1"
    if args.path_style:
        os.environ["RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING"] = "0"

    bucket, prefix = parse_s3_uri(args.model_path)

    # List objects against the origin/direct endpoint (DragonFly may not serve LIST);
    # the object keys are identical on both paths.
    list_client = make_boto3_client(args.endpoint, args.region, args.path_style, args.unsigned)
    objects = list_safetensors_objects(list_client, bucket, prefix)
    if not objects:
        print(f"error: no *.safetensors objects found under {args.model_path}", file=sys.stderr)
        return 1
    keys = [key for key, _ in objects]
    paths = [f"s3://{bucket}/{key}" for key in keys]
    total_size = sum(size for _, size in objects)
    print(
        f"Found {len(objects)} safetensors object(s), {human_bytes(total_size)} total under {args.model_path}"
    )

    summaries: List[RunResult] = []

    if do_direct:
        # Direct path: no priming needed (talks straight to S3).
        direct_results = run_mode(
            label="direct",
            stream_endpoint=args.endpoint,
            paths=paths,
            region=args.region,
            device=args.device,
            iterations=args.iterations,
            warmup=args.warmup,
            prime=False,
            prime_client=None,
            bucket=bucket,
            keys=keys,
        )
        agg = summarize("direct", direct_results)
        if agg:
            summaries.append(agg)

    if do_dragonfly:
        prime_client = make_boto3_client(
            args.dragonfly_endpoint, args.region, args.path_style, args.unsigned
        )
        dragonfly_results = run_mode(
            label="dragonfly",
            stream_endpoint=args.dragonfly_endpoint,
            paths=paths,
            region=args.region,
            device=args.device,
            iterations=args.iterations,
            warmup=args.warmup,
            prime=args.prime,
            prime_client=prime_client,
            bucket=bucket,
            keys=keys,
        )
        agg = summarize("dragonfly", dragonfly_results)
        if agg:
            summaries.append(agg)

    print("\n=== Summary (avg of measured iterations) ===")
    print(f"{'mode':<12}{'avg time':>12}{'throughput':>18}")
    for s in summaries:
        print(f"{s.label:<12}{s.elapsed_s:>10.2f}s{human_bytes(s.throughput_bps) + '/s':>18}")

    if len(summaries) == 2:
        direct, dragonfly = summaries[0], summaries[1]
        if dragonfly.throughput_bps > 0 and direct.throughput_bps > 0:
            speedup = dragonfly.throughput_bps / direct.throughput_bps
            print(f"\nDragonFly is {speedup:.2f}x the direct-S3 throughput.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
