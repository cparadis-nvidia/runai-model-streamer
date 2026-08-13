#!/usr/bin/env python3
"""Re-sign dfdaemon origin GETs with this machine's AWS credentials.

dfdaemon intercepts the streamer's SigV4 request and rewrites Range to a piece
(e.g. bytes=0-33554431) while forwarding the client's signature. S3 then returns
403 SignatureDoesNotMatch.

This process sits on that origin hop only: it drops Authorization / x-amz-* from
the inbound request, keeps dfdaemon's Range, and issues a new signed GetObject.

Wire-up (pick one):

  1. dfdaemon.yaml:
       proxy.rules: [{regex: ".*s3.*amazonaws\\\\.com.*", redirect: "127.0.0.1:9009"}]
     Then set S3_BUCKET (virtual-hosted URLs become path-only after redirect).

  2. HTTP_PROXY=http://127.0.0.1:9009 on the dfdaemon process only
     (streamer still uses HTTP_PROXY=http://127.0.0.1:4001). Absolute-form URLs
     keep the bucket host, so S3_BUCKET is optional.

This process must NOT inherit HTTP_PROXY pointing at dfdaemon, or boto3 loops.
"""
from __future__ import annotations

import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

LISTEN_HOST = os.environ.get("RESIGN_LISTEN_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("RESIGN_LISTEN_PORT", "9009"))
DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-east-1"

# Never send boto3 through dfdaemon / this proxy.
for _proxy_var in (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
):
    os.environ.pop(_proxy_var, None)

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
)


def bucket_and_key(handler: BaseHTTPRequestHandler) -> tuple[str, str]:
    if handler.path.startswith("http://") or handler.path.startswith("https://"):
        parsed = urlparse(handler.path)
    else:
        host = handler.headers.get("Host", "")
        parsed = urlparse(f"http://{host}{handler.path}")

    host = (parsed.hostname or "").split(":")[0]
    key = parsed.path.lstrip("/")
    if host.endswith(".amazonaws.com") and ".s3" in host:
        bucket = host.split(".s3", 1)[0]
    elif host not in ("", "127.0.0.1", "localhost") and not host.startswith("127."):
        bucket = host
    else:
        bucket = DEFAULT_BUCKET
    if not bucket:
        raise ValueError(
            f"cannot derive bucket from host={host!r} path={handler.path!r}; set S3_BUCKET"
        )
    if not key:
        raise ValueError(f"empty object key from path={handler.path!r}")
    return bucket, key


class ResignHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        rng = self.headers.get("Range", "-")
        print(f"{self.command} {self.path} Range={rng} {fmt % args}", flush=True)

    def _send_error_body(self, status: int, message: str) -> None:
        body = message.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        try:
            bucket, key = bucket_and_key(self)
        except ValueError as err:
            self._send_error_body(400, str(err))
            return

        kwargs = {"Bucket": bucket, "Key": key}
        rng = self.headers.get("Range")
        if rng:
            kwargs["Range"] = rng

        print(f"origin GET s3://{bucket}/{key} Range={rng or '-'}", flush=True)
        try:
            obj = s3.get_object(**kwargs)
        except ClientError as err:
            code = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 502)
            msg = err.response.get("Error", {}).get("Message", str(err))
            print(f"S3 error {code}: {msg}", flush=True)
            self._send_error_body(int(code) if code else 502, msg)
            return
        except BotoCoreError as err:
            print(f"boto error: {err}", flush=True)
            self._send_error_body(502, str(err))
            return

        status = 206 if obj.get("ContentRange") else 200
        self.send_response(status)
        if cl := obj.get("ContentLength"):
            self.send_header("Content-Length", str(cl))
        if cr := obj.get("ContentRange"):
            self.send_header("Content-Range", cr)
        if etag := obj.get("ETag"):
            self.send_header("ETag", etag)
        if ctype := obj.get("ContentType"):
            self.send_header("Content-Type", ctype)
        self.send_header("Connection", "close")
        self.end_headers()
        body = obj["Body"]
        try:
            while True:
                chunk = body.read(1024 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)
        finally:
            body.close()


def main() -> int:
    try:
        ident = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()
        print(f"AWS identity {ident.get('Arn', ident)}", flush=True)
    except Exception as err:
        print(f"WARNING: AWS credentials unusable: {err}", file=sys.stderr)
        print(
            'Refresh, then restart this process: eval "$(aws configure export-credentials --format env)"',
            file=sys.stderr,
        )

    httpd = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), ResignHandler)
    print(
        f"s3 re-signer on {LISTEN_HOST}:{LISTEN_PORT} "
        f"region={AWS_REGION} default_bucket={DEFAULT_BUCKET or '(from Host)'}",
        flush=True,
    )
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
