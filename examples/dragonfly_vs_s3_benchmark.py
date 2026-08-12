#!/usr/bin/env python3
"""Benchmark: direct S3 read vs DragonFly + S3 read using the Run:ai Model Streamer.

This script streams every ``*.safetensors`` object under an S3 prefix twice and
compares throughput:

  * ``direct``    - the streamer talks straight to S3 (or your origin endpoint).
  * ``dragonfly`` - the streamer talks to a DragonFly proxy that fronts the same
                    S3 bucket.

Everything goes through the Run:ai Model Streamer SDK - listing, the range(0,0)
primer, and the streamed reads all use the SDK's C++ client (ranged GETs via the
AWS CRT client). Only the endpoint the streamer points at changes between the two
modes (passed as ``S3Credentials.endpoint``).

The DragonFly "range(0,0)" primer
---------------------------------
The streamer reads objects with HTTP ``Range`` requests. When those ranged GETs
hit DragonFly *cold* (object not yet known to DragonFly), DragonFly first has to
learn the object size, so it synthesizes its own ``Range: bytes=0-0`` probe toward
the origin. That synthesized probe reuses the caller's SigV4 authorization but with
a mutated ``Range`` header, so the request no longer matches the signature the
client computed -> S3 rejects it (``SignatureDoesNotMatch``) and the read fails.

The fix, per Omer Dayan: issue an explicit, correctly-signed ``Range: bytes=0-0``
GET for each object *ourselves* before the real streamed reads. Crucially this is
done through the SDK (a 1-byte range at offset 0 -> ``bytes=0-0``), so it is the
*same* streamer client - same connection pool and same SigV4 signing - that then
does the real reads. DragonFly learns the object metadata from a valid request and
never has to forge its own broken probe. This priming step is ON by default for
the ``dragonfly`` path and can be turned off with ``--no-prime`` (to reproduce the
failure).

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
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

SAFETENSORS_PATTERN = "*.safetensors"


def _credentials(endpoint: Optional[str], region: Optional[str]):
    """Build S3Credentials carrying only the endpoint/region override.

    The access keys themselves are left unset so the C++ layer resolves them from
    the ambient AWS credential chain - identical for direct and DragonFly.
    """
    from runai_model_streamer.s3_utils.s3_utils import S3Credentials

    return S3Credentials(region_name=region, endpoint=endpoint)


def list_safetensors_via_sdk(
    prefix: str, endpoint: Optional[str], region: Optional[str]
) -> List[Tuple[str, int]]:
    """List every ``*.safetensors`` object under ``prefix`` using the SDK.

    Listing is done against the origin/direct endpoint (DragonFly may not serve
    LIST); the object keys are identical on both paths.
    """
    from runai_model_streamer import FileStreamer

    credentials = _credentials(endpoint, region)
    with FileStreamer() as streamer:
        # Apply the endpoint/region credentials to this streamer before listing so
        # a custom origin endpoint is honored (set-once, streamer-scoped).
        streamer.handle_object_store(prefix, streamer.streamer, credentials)
        return streamer.list_files(
            prefix, is_recursive=True, allow_patterns=[SAFETENSORS_PATTERN]
        )


def prime_range_0_0_via_sdk(
    paths: List[str], endpoint: Optional[str], region: Optional[str], device: str
) -> None:
    """Issue an explicit ``Range: bytes=0-0`` GET per object *through the SDK*.

    A 1-byte range at offset 0 becomes ``bytes=0-0`` on the wire. Running it through
    the same streamer client that does the real reads means DragonFly caches the
    object metadata from a correctly-signed request and never forges its own broken
    range(0,0) probe. See the module docstring for the full rationale.
    """
    from runai_model_streamer import FileStreamer, FileChunks

    credentials = _credentials(endpoint, region)
    requests = [
        FileChunks.contiguous(i, path, 0, [1]) for i, path in enumerate(paths)
    ]
    with FileStreamer() as streamer:
        streamer.stream_files(requests, credentials=credentials, device=device)
        # Drain the single 1-byte response per file so the primer completes.
        for _path, _chunk_index, _tensor in streamer.get_chunks():
            pass


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
    """Stream all paths through the Run:ai Model Streamer SDK and time it.

    The endpoint override is what selects direct-S3 vs DragonFly: it is passed as
    ``S3Credentials.endpoint``, which the C++ layer applies as the CRT client's
    endpointOverride. Credentials themselves come from the ambient AWS chain.
    """
    from runai_model_streamer import SafetensorsStreamer

    credentials = _credentials(endpoint, region)

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
) -> List[RunResult]:
    print(f"\n=== {label} ({len(paths)} file(s), endpoint={stream_endpoint or 'default S3'}) ===")
    results: List[RunResult] = []
    total_iters = warmup + iterations
    for i in range(total_iters):
        is_warmup = i < warmup
        tag = "warmup" if is_warmup else f"iter {i - warmup + 1}/{iterations}"

        if prime:
            print(f"  [{tag}] priming range(0,0) for {len(paths)} object(s) via {label} endpoint ...")
            prime_range_0_0_via_sdk(paths, stream_endpoint, region, device)

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
    return RunResult(label=label, elapsed_s=avg_elapsed, bytes_streamed=bytes_streamed)


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
        help="use path-style addressing (S3-compatible endpoints); sets RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0",
    )
    parser.add_argument(
        "--unsigned",
        action="store_true",
        help="use anonymous/unsigned requests (public buckets); sets RUNAI_STREAMER_S3_UNSIGNED=1",
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

    objects = list_safetensors_via_sdk(args.model_path, args.endpoint, args.region)
    if not objects:
        print(f"error: no *.safetensors objects found under {args.model_path}", file=sys.stderr)
        return 1
    paths = [path for path, _ in objects]
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
        )
        agg = summarize("direct", direct_results)
        if agg:
            summaries.append(agg)

    if do_dragonfly:
        dragonfly_results = run_mode(
            label="dragonfly",
            stream_endpoint=args.dragonfly_endpoint,
            paths=paths,
            region=args.region,
            device=args.device,
            iterations=args.iterations,
            warmup=args.warmup,
            prime=args.prime,
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
