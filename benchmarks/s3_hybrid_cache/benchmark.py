#!/usr/bin/env python3
"""A/B benchmark for Run:ai Model Streamer with Hybrid Cache for Amazon S3."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import resource
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
)


def _worker_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--s3-uri", action="append", required=True)
    return parser


def _max_rss_mib() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB; macOS reports bytes.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return rss / divisor


def run_worker(argv: Sequence[str]) -> int:
    args = _worker_parser().parse_args(argv)

    # Import only after the parent has constructed this arm's environment. The
    # native AWS client reads endpoint and proxy configuration during startup.
    import torch
    from importlib import metadata
    from runai_model_streamer import SafetensorsStreamer

    if args.device.lower().startswith("cuda") and not torch.cuda.is_available():
        raise RuntimeError(f"CUDA device requested but CUDA is unavailable: {args.device}")

    if args.device.lower().startswith("cuda"):
        torch.cuda.reset_peak_memory_stats(args.device)

    started = time.perf_counter()
    tensor_count = 0
    tensor_bytes = 0
    with SafetensorsStreamer() as streamer:
        streamer.stream_files(args.s3_uri, device=args.device)
        for _, tensor in streamer.get_tensors():
            tensor_count += 1
            tensor_bytes += tensor.numel() * tensor.element_size()
            del tensor

    if args.device.lower().startswith("cuda"):
        torch.cuda.synchronize(args.device)
    elapsed = time.perf_counter() - started

    try:
        streamer_version = metadata.version("runai-model-streamer")
    except metadata.PackageNotFoundError:
        streamer_version = "unknown"

    result: Dict[str, Any] = {
        "elapsed_seconds": elapsed,
        "tensor_count": tensor_count,
        "tensor_bytes": tensor_bytes,
        "throughput_gib_per_second": tensor_bytes / elapsed / (1024 ** 3),
        "max_rss_mib": _max_rss_mib(),
        "device": args.device,
        "python": platform.python_version(),
        "torch": torch.__version__,
        "runai_model_streamer": streamer_version,
    }
    if args.device.lower().startswith("cuda"):
        result["max_cuda_memory_mib"] = (
            torch.cuda.max_memory_allocated(args.device) / (1024 ** 2)
        )

    Path(args.result_file).write_text(json.dumps(result, indent=2) + "\n")
    return 0


def _main_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare direct S3 model streaming with cold and warm reads through "
            "Hybrid Cache for Amazon S3."
        )
    )
    model = parser.add_mutually_exclusive_group(required=True)
    model.add_argument(
        "--s3-uri",
        action="append",
        help="Exact s3:// URI of a safetensors shard; repeat for multiple shards.",
    )
    model.add_argument(
        "--manifest",
        type=Path,
        help="Text file containing one exact s3:// safetensors URI per line.",
    )
    parser.add_argument("--region", required=True, help="AWS region containing the bucket.")
    parser.add_argument(
        "--proxy-binary",
        required=True,
        type=Path,
        help="Built sample-s3-hybrid-cache target/release/s3-proxy binary.",
    )
    parser.add_argument("--device", default="cpu", help="Torch device, for example cpu or cuda:0.")
    parser.add_argument("--direct-runs", type=int, default=3)
    parser.add_argument("--warm-runs", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--chunk-mib", type=int, default=8)
    parser.add_argument("--cache-size-gib", type=float, default=32.0)
    parser.add_argument("--ram-cache-gib", type=float, default=1.0)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="New results directory. Defaults below this benchmark's results/ directory.",
    )
    parser.add_argument("--worker-timeout-seconds", type=int, default=3600)
    return parser


def _load_uris(args: argparse.Namespace) -> List[str]:
    if args.s3_uri:
        raw = args.s3_uri
    else:
        raw = [
            line.strip()
            for line in args.manifest.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

    uris = list(dict.fromkeys(raw))
    if not uris:
        raise ValueError("No model URIs were provided")
    invalid = [uri for uri in uris if not uri.startswith("s3://")]
    if invalid:
        raise ValueError(f"Every model URI must start with s3://; invalid: {invalid}")
    return uris


def _bucket_names(uris: Iterable[str]) -> List[str]:
    return sorted({uri[5:].split("/", 1)[0] for uri in uris})


def _allocate_ports(count: int) -> List[int]:
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def _yaml_string(value: Path) -> str:
    # A JSON string is also a valid YAML scalar and handles spaces safely.
    return json.dumps(str(value.resolve()))


def _write_proxy_config(
    output_dir: Path,
    ports: Tuple[int, int, int, int],
    cache_size_bytes: int,
    ram_cache_size_bytes: int,
) -> Path:
    proxy_port, health_port, metrics_port, dashboard_port = ports
    cache_dir = output_dir / "cache"
    access_logs = output_dir / "proxy-logs" / "access"
    app_logs = output_dir / "proxy-logs" / "app"
    config = f"""server:
  mode: "proxy_only"
  proxy_port: {proxy_port}
  max_concurrent_requests: 512
  request_timeout: "120s"
cache:
  cache_dir: {_yaml_string(cache_dir)}
  max_cache_size: {cache_size_bytes}
  ram_cache_enabled: true
  max_ram_cache_size: {ram_cache_size_bytes}
  ram_cache_shard_count: 8
  get_ttl: "1h"
  head_ttl: "1h"
compression:
  enabled: false
  threshold: 1024
  preferred_algorithm: "lz4"
logging:
  access_log_dir: {_yaml_string(access_logs)}
  app_log_dir: {_yaml_string(app_logs)}
  access_log_enabled: true
  log_level: "info"
health:
  enabled: true
  endpoint: "/health"
  port: {health_port}
  bind_address: "127.0.0.1"
  check_interval: "30s"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: {metrics_port}
  bind_address: "127.0.0.1"
  collection_interval: "1s"
dashboard:
  enabled: false
  port: {dashboard_port}
  bind_address: "127.0.0.1"
"""
    path = output_dir / "hybrid-cache.yaml"
    path.write_text(config)
    return path


def _local_get_json(url: str, timeout: float = 5.0) -> Dict[str, Any]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_proxy(process: subprocess.Popen, health_url: str, log_path: Path) -> None:
    deadline = time.monotonic() + 30
    last_error: Optional[Exception] = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = log_path.read_text(errors="replace")[-4000:]
            raise RuntimeError(f"Hybrid Cache exited during startup:\n{tail}")
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(health_url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:  # startup polling intentionally tolerates connection errors
            last_error = exc
        time.sleep(0.25)
    raise TimeoutError(f"Hybrid Cache did not become healthy: {last_error}")


def _stop_proxy(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _base_worker_env(args: argparse.Namespace) -> Dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "AWS_REGION": args.region,
            "AWS_DEFAULT_REGION": args.region,
            "RUNAI_STREAMER_CONCURRENCY": str(args.concurrency),
            "RUNAI_STREAMER_CHUNK_BYTESIZE": str(args.chunk_mib * 1024 * 1024),
            "RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING": "1",
        }
    )
    return env


def _direct_env(args: argparse.Namespace) -> Dict[str, str]:
    env = _base_worker_env(args)
    for key in PROXY_ENV_KEYS:
        env.pop(key, None)
    env.pop("AWS_ENDPOINT_URL", None)
    env.pop("AWS_ENDPOINT_URL_S3", None)
    env["RUNAI_STREAMER_S3_USE_SYSTEM_PROXY"] = "0"
    return env


def _cached_env(args: argparse.Namespace, proxy_url: str) -> Dict[str, str]:
    env = _base_worker_env(args)
    for key in PROXY_ENV_KEYS:
        env.pop(key, None)
    endpoint = f"http://s3.{args.region}.amazonaws.com"
    env.update(
        {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ENDPOINT_URL_S3": endpoint,
            "RUNAI_STREAMER_OVERRIDE_ENDPOINT_URL": "1",
            "RUNAI_STREAMER_S3_USE_SYSTEM_PROXY": "1",
            "HTTP_PROXY": proxy_url,
            "http_proxy": proxy_url,
            "NO_PROXY": "169.254.169.254,127.0.0.1,localhost",
            "no_proxy": "169.254.169.254,127.0.0.1,localhost",
        }
    )
    return env


def _run_sample(
    args: argparse.Namespace,
    uris: List[str],
    output_dir: Path,
    scenario: str,
    repetition: int,
    env: Dict[str, str],
) -> Dict[str, Any]:
    stem = f"{scenario}-{repetition:02d}"
    result_path = output_dir / "samples" / f"{stem}.json"
    log_path = output_dir / "samples" / f"{stem}.log"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "_worker",
        "--result-file",
        str(result_path),
        "--device",
        args.device,
    ]
    for uri in uris:
        command.extend(("--s3-uri", uri))

    completed = subprocess.run(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=args.worker_timeout_seconds,
        check=False,
    )
    log_path.write_text(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(
            f"{scenario} run {repetition} failed (see {log_path}):\n"
            f"{completed.stdout[-4000:]}"
        )
    sample = json.loads(result_path.read_text())
    sample.update({"scenario": scenario, "repetition": repetition})
    return sample


def _metrics_snapshot(metrics_url: str, buckets: List[str]) -> Dict[str, float]:
    payload = _local_get_json(metrics_url)
    cache = payload.get("cache") or {}
    traffic = payload.get("bucket_traffic") or {}
    snapshot: Dict[str, float] = {}
    for name in (
        "cache_hits",
        "cache_misses",
        "ram_cache_hits",
        "ram_cache_misses",
        "metadata_cache_hits",
        "metadata_cache_misses",
        "total_cache_size",
    ):
        snapshot[name] = float(cache.get(name, 0))
    for name in ("bytes_served", "bytes_saved", "get_requests"):
        snapshot[name] = sum(float((traffic.get(bucket) or {}).get(name, 0)) for bucket in buckets)
    return snapshot


def _subtract_metrics(after: Dict[str, float], before: Dict[str, float]) -> Dict[str, float]:
    return {key: after.get(key, 0) - before.get(key, 0) for key in after}


def _summaries(samples: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    summaries: Dict[str, Dict[str, float]] = {}
    for scenario in ("direct_s3", "hybrid_cache_fill", "hybrid_cache_warm"):
        group = [sample for sample in samples if sample["scenario"] == scenario]
        if not group:
            continue
        elapsed = [float(sample["elapsed_seconds"]) for sample in group]
        throughput = [float(sample["throughput_gib_per_second"]) for sample in group]
        summaries[scenario] = {
            "runs": len(group),
            "median_seconds": statistics.median(elapsed),
            "mean_seconds": statistics.mean(elapsed),
            "min_seconds": min(elapsed),
            "max_seconds": max(elapsed),
            "mean_throughput_gib_per_second": statistics.mean(throughput),
        }
    direct = summaries.get("direct_s3")
    warm = summaries.get("hybrid_cache_warm")
    if direct and warm:
        speedup = direct["median_seconds"] / warm["median_seconds"]
        summaries["comparison"] = {
            "warm_cache_speedup_x": speedup,
            "warm_cache_time_reduction_percent": (1 - 1 / speedup) * 100,
        }
    return summaries


def _write_csv(path: Path, samples: List[Dict[str, Any]]) -> None:
    fields = (
        "scenario",
        "repetition",
        "elapsed_seconds",
        "throughput_gib_per_second",
        "tensor_count",
        "tensor_bytes",
        "max_rss_mib",
        "max_cuda_memory_mib",
    )
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(samples)


def _print_summary(summaries: Dict[str, Dict[str, float]], verified: bool) -> None:
    print("\nScenario                 Runs   Median(s)   Mean GiB/s")
    print("-----------------------  ----   ---------   ----------")
    for scenario in ("direct_s3", "hybrid_cache_fill", "hybrid_cache_warm"):
        summary = summaries.get(scenario)
        if summary:
            print(
                f"{scenario:23}  {int(summary['runs']):4d}   "
                f"{summary['median_seconds']:9.3f}   "
                f"{summary['mean_throughput_gib_per_second']:10.3f}"
            )
    comparison = summaries.get("comparison")
    if comparison:
        print(f"\nWarm-cache speedup vs direct S3: {comparison['warm_cache_speedup_x']:.2f}x")
    print(f"Hybrid Cache bytes_saved verification: {'PASS' if verified else 'FAIL'}")


def run_benchmark(argv: Sequence[str]) -> int:
    args = _main_parser().parse_args(argv)
    if min(args.direct_runs, args.warm_runs, args.concurrency, args.chunk_mib) <= 0:
        raise ValueError("Run counts, concurrency, and chunk size must be positive")
    if min(args.cache_size_gib, args.ram_cache_gib) <= 0:
        raise ValueError("Cache sizes must be positive")

    uris = _load_uris(args)
    proxy_binary = args.proxy_binary.resolve()
    if not proxy_binary.is_file() or not os.access(proxy_binary, os.X_OK):
        raise ValueError(f"Proxy binary is missing or not executable: {proxy_binary}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (args.output_dir or (Path(__file__).parent / "results" / timestamp)).resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / "samples").mkdir()

    ports = tuple(_allocate_ports(4))
    proxy_port, health_port, metrics_port, _ = ports
    config_path = _write_proxy_config(
        output_dir,
        ports,
        int(args.cache_size_gib * (1024 ** 3)),
        int(args.ram_cache_gib * (1024 ** 3)),
    )
    proxy_url = f"http://127.0.0.1:{proxy_port}"
    health_url = f"http://127.0.0.1:{health_port}/health"
    metrics_url = f"http://127.0.0.1:{metrics_port}/metrics"
    buckets = _bucket_names(uris)
    proxy_log_path = output_dir / "hybrid-cache-process.log"

    samples: List[Dict[str, Any]] = []
    proxy_log = proxy_log_path.open("w")
    process = subprocess.Popen(
        [str(proxy_binary), "-c", str(config_path)],
        stdout=proxy_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_proxy(process, health_url, proxy_log_path)
        before_fill = _metrics_snapshot(metrics_url, buckets)
        samples.append(
            _run_sample(
                args,
                uris,
                output_dir,
                "hybrid_cache_fill",
                1,
                _cached_env(args, proxy_url),
            )
        )
        after_fill = _metrics_snapshot(metrics_url, buckets)

        # Alternate the two measured arms to reduce bias from time-varying network
        # conditions. The cache-fill sample is reported separately.
        for index in range(max(args.direct_runs, args.warm_runs)):
            order = ("direct_s3", "hybrid_cache_warm")
            if index % 2:
                order = tuple(reversed(order))
            for scenario in order:
                if scenario == "direct_s3" and index < args.direct_runs:
                    samples.append(
                        _run_sample(
                            args,
                            uris,
                            output_dir,
                            scenario,
                            index + 1,
                            _direct_env(args),
                        )
                    )
                elif scenario == "hybrid_cache_warm" and index < args.warm_runs:
                    samples.append(
                        _run_sample(
                            args,
                            uris,
                            output_dir,
                            scenario,
                            index + 1,
                            _cached_env(args, proxy_url),
                        )
                    )
        after_all = _metrics_snapshot(metrics_url, buckets)
    finally:
        _stop_proxy(process)
        proxy_log.close()

    fill_metrics = _subtract_metrics(after_fill, before_fill)
    warm_metrics = _subtract_metrics(after_all, after_fill)
    verified = warm_metrics.get("bytes_saved", 0) > 0
    summaries = _summaries(samples)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "configuration": {
            "region": args.region,
            "device": args.device,
            "model_uris": uris,
            "concurrency": args.concurrency,
            "chunk_mib": args.chunk_mib,
            "cache_size_gib": args.cache_size_gib,
            "ram_cache_gib": args.ram_cache_gib,
            "proxy_binary": str(proxy_binary),
            "proxy_config": str(config_path),
        },
        "cache_metrics": {
            "fill_delta": fill_metrics,
            "measured_runs_delta": warm_metrics,
            "bytes_saved_verified": verified,
        },
        "samples": samples,
        "summary": summaries,
    }
    (output_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    _write_csv(output_dir / "samples.csv", samples)
    _print_summary(summaries, verified)
    print(f"Results: {output_dir}")
    return 0 if verified else 2


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "_worker":
        return run_worker(sys.argv[2:])
    return run_benchmark(sys.argv[1:])


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
