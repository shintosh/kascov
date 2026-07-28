#!/usr/bin/env python3
"""Verify that Kascov Caddy routes preserve native EventSource semantics."""

from __future__ import annotations

import http.client
import http.server
import pathlib
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
CONFIGS = (ROOT / "scripts/kascov.Caddyfile", ROOT / "scripts/kascov.windows.Caddyfile")


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class StreamHandler(http.server.BaseHTTPRequestHandler):
    last_event_id = ""
    received = threading.Event()

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        type(self).last_event_id = self.headers.get("Last-Event-ID", "")
        type(self).received.set()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b"event: ready\ndata: one\n\n")
        self.wfile.flush()
        time.sleep(0.5)
        self.wfile.write(b"event: accepted\ndata: two\n\n")
        self.wfile.flush()

    def log_message(self, _format: str, *_args: object) -> None:
        return


class CaddyEventSourceTest(unittest.TestCase):
    def test_production_configs_disable_compression_and_flush_streams(self) -> None:
        for path in CONFIGS:
            with self.subTest(config=path.name):
                text = path.read_text(encoding="utf-8")
                self.assertIn("@not_stream not path /data/*/stream", text)
                self.assertIn("encode @not_stream zstd gzip", text)
                self.assertIn("flush_interval -1", text)
                self.assertIn(
                    "header_up Last-Event-ID {http.request.header.Last-Event-ID}",
                    text,
                )

    @unittest.skipUnless(shutil.which("caddy"), "caddy is not installed")
    def test_live_proxy_flushes_without_compression_and_preserves_cursor(self) -> None:
        upstream_port = free_port()
        proxy_port = free_port()
        StreamHandler.last_event_id = ""
        StreamHandler.received.clear()
        server = http.server.ThreadingHTTPServer(("127.0.0.1", upstream_port), StreamHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        with tempfile.TemporaryDirectory(prefix="kascov-caddy-") as temporary:
            config = pathlib.Path(temporary) / "Caddyfile"
            config.write_text(
                f"""{{
    auto_https off
    admin off
}}
http://127.0.0.1:{proxy_port} {{
    @not_stream not path /data/*/stream
    encode @not_stream gzip
    @stream path /data/*/stream
    handle @stream {{
        reverse_proxy 127.0.0.1:{upstream_port} {{
            flush_interval -1
            header_up Last-Event-ID {{http.request.header.Last-Event-ID}}
        }}
    }}
}}
""",
                encoding="utf-8",
            )
            process = subprocess.Popen(
                ["caddy", "run", "--config", str(config), "--adapter", "caddyfile"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 3
                while True:
                    try:
                        with socket.create_connection(("127.0.0.1", proxy_port), timeout=0.1):
                            break
                    except OSError:
                        if process.poll() is not None or time.monotonic() >= deadline:
                            stderr = process.stderr.read() if process.stderr else ""
                            self.fail(f"caddy did not start: {stderr[-1000:]}")
                        time.sleep(0.02)

                connection = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=2)
                started = time.monotonic()
                connection.request(
                    "GET",
                    "/data/testnet-10/stream",
                    headers={
                        "Accept": "text/event-stream",
                        "Accept-Encoding": "gzip",
                        "Last-Event-ID": "00112233445566778899aabbccddeeff:42",
                    },
                )
                response = connection.getresponse()
                first_line = response.readline()
                elapsed = time.monotonic() - started
                self.assertEqual(200, response.status)
                self.assertEqual(b"event: ready\n", first_line)
                self.assertLess(elapsed, 0.4)
                self.assertIsNone(response.getheader("Content-Encoding"))
                self.assertTrue(StreamHandler.received.wait(timeout=1))
                self.assertEqual(
                    "00112233445566778899aabbccddeeff:42",
                    StreamHandler.last_event_id,
                )
                connection.close()
            finally:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                if process.stderr:
                    process.stderr.close()
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
