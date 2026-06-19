#!/usr/bin/env python3
"""Tiny HTTP proxy that adds latency and optional bandwidth shaping.

Use it to simulate a remote ripclone server without leaving your laptop:

  python3 scripts/latency_proxy.py 127.0.0.1:8000 127.0.0.1:9000 0.05
  python3 scripts/latency_proxy.py 127.0.0.1:8000 127.0.0.1:9000 0.01 1000

The proxy listens on the first address and forwards to the second. It sleeps
for `latency` seconds before forwarding the request and again before sending
back the response, and it can cap aggregate throughput with a token bucket.
"""
import http.client
import http.server
import socketserver
import sys
import threading
import time


def parse_addr(addr: str):
    host, port = addr.rsplit(":", 1)
    return host, int(port)


class TokenBucket:
    """Global token bucket for aggregate bandwidth shaping."""

    def __init__(self, bytes_per_sec: float, max_burst_seconds: float = 1.0):
        self.rate = bytes_per_sec
        self.max_tokens = bytes_per_sec * max_burst_seconds
        self.tokens = self.max_tokens
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def consume(self, nbytes: int):
        if self.rate <= 0:
            return
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.tokens + elapsed * self.rate, self.max_tokens)
            if self.tokens < nbytes:
                need = nbytes - self.tokens
                sleep = need / self.rate
                time.sleep(sleep)
                self.tokens += sleep * self.rate
            self.tokens -= nbytes


class LatencyProxy(http.server.BaseHTTPRequestHandler):
    upstream_host: str
    upstream_port: int
    latency: float
    bucket: "TokenBucket | None" = None

    def log_message(self, fmt, *args):
        pass

    def _proxy(self):
        time.sleep(self.latency)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else None
        if body:
            if self.bucket:
                self.bucket.consume(len(body))

        conn = http.client.HTTPConnection(self.upstream_host, self.upstream_port)
        headers = {k: v for k, v in self.headers.items() if k.lower() != "host"}
        headers["Host"] = f"{self.upstream_host}:{self.upstream_port}"
        try:
            conn.request(self.command, self.path, body=body, headers=headers)
            resp = conn.getresponse()
            resp_body = resp.read()
        finally:
            conn.close()

        if self.bucket:
            self.bucket.consume(len(resp_body))
        time.sleep(self.latency)
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "content-length", "connection"):
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        self.wfile.write(resp_body)

    def do_GET(self):
        self._proxy()

    def do_POST(self):
        self._proxy()

    def do_HEAD(self):
        self._proxy()


def main():
    if len(sys.argv) not in (4, 5):
        print(
            f"usage: {sys.argv[0]} <listen_addr> <upstream_addr> <latency_seconds> [bandwidth_mbps]",
            file=sys.stderr,
        )
        sys.exit(1)

    listen_host, listen_port = parse_addr(sys.argv[1])
    upstream_host, upstream_port = parse_addr(sys.argv[2])
    latency = float(sys.argv[3])
    bandwidth_mbps = float(sys.argv[4]) if len(sys.argv) == 5 else 0.0

    LatencyProxy.upstream_host = upstream_host
    LatencyProxy.upstream_port = upstream_port
    LatencyProxy.latency = latency
    if bandwidth_mbps > 0:
        LatencyProxy.bucket = TokenBucket(bandwidth_mbps * 1_000_000 / 8)
        bw_text = f"{bandwidth_mbps} Mbps"
    else:
        LatencyProxy.bucket = None
        bw_text = "unlimited"

    with socketserver.ThreadingTCPServer((listen_host, listen_port), LatencyProxy) as srv:
        print(
            f"proxy listening on {listen_host}:{listen_port} -> {upstream_host}:{upstream_port} "
            f"(latency {latency}s, bandwidth {bw_text})"
        )
        srv.serve_forever()


if __name__ == "__main__":
    main()
