#!/usr/bin/env python3
"""Stream a model with the Run:ai Model Streamer and report time + throughput.

This is the parametrized version of the ``model-streamer-only.py`` script from the
"RunAI Model Streamer Caching Benchmarks" doc. It streams a set of safetensors
shards and times the full read, exactly like the doc's reproduction script:

    with SafetensorsStreamer() as streamer:
        streamer.stream_files(file_paths)
        for name, tensor in streamer.get_tensors():
            continue

Caching is selected purely by environment, matching the doc:

  * No cache   - stream straight from S3 (default).
  * With cache - route through the local cache proxy (S3 Hybrid Cache or Dragonfly,
                 whichever binary is running in ``proxy_only`` mode on the proxy
                 port). This sets, per the doc:

                     AWS_ENDPOINT_URL = http://s3.<region>.amazonaws.com
                     HTTP_PROXY / HTTPS_PROXY = http://127.0.0.1:3128

    The cache type (S3 Hybrid vs Dragonfly) is not a client-side setting - it is
    whichever proxy is listening on the proxy port. From the streamer's point of
    view the two are identical HTTP forward proxies.

Defaults reproduce the doc's single-replica run: Falcon-40b, 9 shards under
``s3://core-llm/falcon-40b``.

Examples
--------
    # No cache (default):
    python model_streamer_stream.py

    # With cache (proxy on 127.0.0.1:3128):
    python model_streamer_stream.py --proxy http://127.0.0.1:3128 \
        --endpoint http://s3.us-east-1.amazonaws.com

    # Nth replica, clean page cache first (needs sudo):
    python model_streamer_stream.py --proxy http://127.0.0.1:3128 \
        --endpoint http://s3.us-east-1.amazonaws.com --drop-page-cache
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from typing import List, Optional

DEFAULT_ROOT = "s3://core-llm/falcon-40b"
DEFAULT_NUM_SHARDS = 9


def build_file_paths(root: str, num_shards: int) -> List[str]:
    """Reproduce the doc's shard naming: model-000NN-of-000TT.safetensors."""
    root = root.rstrip("/")
    return [
        f"{root}/model-{i + 1:05d}-of-{num_shards:05d}.safetensors"
        for i in range(num_shards)
    ]


def drop_page_cache() -> None:
    """Clean the OS page cache: sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'."""
    print("Dropping page cache (echo 3 > /proc/sys/vm/drop_caches) ...", flush=True)
    subprocess.run(
        ["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
        check=True,
    )


def prime_range_0_0(file_paths: List[str], device: str) -> None:
    """Optional: issue an explicit Range: bytes=0-0 GET per object through the SDK.

    NOT part of the doc's baseline. Some Dragonfly proxy setups otherwise synthesize
    their own range(0,0) probe toward the origin, which mutates the SigV4-signed
    Range header and gets rejected by S3 (SignatureDoesNotMatch). Doing it here (a
    1-byte range at offset 0 -> bytes=0-0) uses the same streamer client that then
    does the real reads, so the proxy caches object metadata from a valid request.
    """
    from runai_model_streamer import FileStreamer, FileChunks

    requests = [FileChunks.contiguous(i, p, 0, [1]) for i, p in enumerate(file_paths)]
    with FileStreamer() as streamer:
        streamer.stream_files(requests, device=device)
        for _p, _c, _t in streamer.get_chunks():
            pass


def human_bytes(n: float) -> str:
    step = 1024.0
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < step:
            return f"{n:.3f} {unit}"
        n /= step
    return f"{n:.3f} PiB"


def stream(file_paths: List[str], device: str) -> None:
    from runai_model_streamer import SafetensorsStreamer

    t0 = time.perf_counter()
    print(f"{t0:.3f} s", flush=True)
    with SafetensorsStreamer() as streamer:
        streamer.stream_files(file_paths, device=device)
        for _name, _tensor in streamer.get_tensors():
            continue
        bytes_streamed = streamer.total_size
    t1 = time.perf_counter()
    elapsed = t1 - t0
    print(f"{t1:.3f} s", flush=True)
    print(f"{elapsed:.3f} s", flush=True)

    if bytes_streamed and elapsed > 0:
        throughput = bytes_streamed / elapsed
        print(
            f"Streamed {human_bytes(bytes_streamed)} in {elapsed:.3f}s "
            f"-> {human_bytes(throughput)}/s",
            flush=True,
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--root", default=DEFAULT_ROOT, help=f"model prefix (default: {DEFAULT_ROOT})")
    parser.add_argument(
        "--num-shards", type=int, default=DEFAULT_NUM_SHARDS,
        help=f"number of safetensors shards (default: {DEFAULT_NUM_SHARDS})",
    )
    parser.add_argument(
        "--file", action="append", dest="files", default=None,
        help="explicit s3:// shard path (repeatable); overrides --root/--num-shards",
    )
    parser.add_argument(
        "--proxy", default=None,
        help="cache proxy URL, e.g. http://127.0.0.1:3128 (sets HTTP_PROXY/HTTPS_PROXY)",
    )
    parser.add_argument(
        "--endpoint", default=None,
        help="AWS_ENDPOINT_URL, e.g. http://s3.us-east-1.amazonaws.com (use with --proxy)",
    )
    parser.add_argument("--device", default="cpu", help="destination device (default: cpu)")
    parser.add_argument(
        "--drop-page-cache", action="store_true",
        help="drop the OS page cache before streaming (needs sudo) - the 'clean PageCache' scenario",
    )
    parser.add_argument(
        "--prime-range0", action="store_true",
        help="issue an explicit Range: bytes=0-0 GET per object first (not in the doc baseline; see docstring)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    # Cache selection is environment-only, matching the doc. Set before any streaming.
    if args.endpoint:
        os.environ["AWS_ENDPOINT_URL"] = args.endpoint
    if args.proxy:
        os.environ["HTTP_PROXY"] = args.proxy
        os.environ["HTTPS_PROXY"] = args.proxy

    file_paths = args.files if args.files else build_file_paths(args.root, args.num_shards)
    print(f"Streaming {len(file_paths)} file(s); proxy={args.proxy or 'none'}, "
          f"endpoint={os.environ.get('AWS_ENDPOINT_URL', 'default')}", flush=True)

    if args.drop_page_cache:
        drop_page_cache()

    if args.prime_range0:
        prime_range_0_0(file_paths, args.device)

    stream(file_paths, args.device)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
