#!/usr/bin/env python3
"""Seed a MinIO bucket with synthetic safetensors shards for the benchmark.

Generates ``--num-shards`` safetensors files of ``--shard-mib`` each (real, valid
safetensors payloads) and uploads them to a MinIO bucket under a prefix, so the
streaming benchmark has something to read without needing real S3 permissions.

The shard names match what ``model_streamer_stream.py`` expects by default:
``model-000NN-of-000TT.safetensors``.

Example
-------
    python seed_minio.py \
        --endpoint http://127.0.0.1:9000 \
        --bucket core-llm --prefix falcon-40b \
        --num-shards 9 --shard-mib 256

Then stream it:

    python ../model_streamer_stream.py \
        --root s3://core-llm/falcon-40b --num-shards 9 \
        --endpoint http://127.0.0.1:9000 --path-style \
        --access-key minioadmin --secret-key minioadmin --region us-east-1

Note: these are synthetic shards, not the real Falcon-40b weights - use them to
exercise the Direct-vs-Dragonfly path locally, then scale --shard-mib/--num-shards
toward the real model size as needed.
"""

from __future__ import annotations

import argparse
import os
import tempfile
from typing import Optional, List


def make_shard(path: str, shard_bytes: int) -> None:
    """Write one safetensors file of ~shard_bytes as a single float32 tensor."""
    import torch
    from safetensors.torch import save_file

    elements = max(1, shard_bytes // 4)  # float32 = 4 bytes
    tensor = torch.arange(elements, dtype=torch.float32)
    save_file({"weight": tensor}, path)


def build_client(endpoint: str, region: str, access_key: str, secret_key: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(client, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError:
        pass
    client.create_bucket(Bucket=bucket)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--bucket", default="core-llm")
    parser.add_argument("--prefix", default="falcon-40b")
    parser.add_argument("--num-shards", type=int, default=9)
    parser.add_argument("--shard-mib", type=int, default=256, help="size of each shard in MiB (default: 256)")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args(argv)

    client = build_client(args.endpoint, args.region, args.access_key, args.secret_key)
    ensure_bucket(client, args.bucket)

    shard_bytes = args.shard_mib * 1024 * 1024
    prefix = args.prefix.strip("/")
    total = 0
    with tempfile.TemporaryDirectory() as tmp:
        for i in range(args.num_shards):
            name = f"model-{i + 1:05d}-of-{args.num_shards:05d}.safetensors"
            local = os.path.join(tmp, name)
            make_shard(local, shard_bytes)
            key = f"{prefix}/{name}"
            client.upload_file(local, args.bucket, key)
            size = os.path.getsize(local)
            total += size
            os.remove(local)
            print(f"  uploaded s3://{args.bucket}/{key} ({size / 1024 / 1024:.1f} MiB)", flush=True)

    print(
        f"Seeded {args.num_shards} shard(s), {total / 1024 / 1024 / 1024:.2f} GiB total, "
        f"to s3://{args.bucket}/{prefix}/"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
