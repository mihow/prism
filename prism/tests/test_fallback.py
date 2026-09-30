"""Tests for reading originals from a fallback origin and copying them back to the read bucket.

HTTP is stubbed: origins._session is replaced with FakeHttp, which answers per origin and
records every request, so these tests run without S3, minio or the network. The two tests in
TestRealHttp use a local HTTP server and a closed local port instead, to exercise the real
retry adapter and timeouts.
"""
import base64
import hashlib
import http.server
import logging
import os
import threading
import time
import unittest
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Dict, List
from unittest import mock

import requests
from requests.structures import CaseInsensitiveDict
from werkzeug.exceptions import BadGateway, BadRequest, InternalServerError, NotFound
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request

# prism.app builds a credentials store at import time and needs one of these set.
os.environ.setdefault("S3_BUCKET", "prism-test")

import sentry_sdk  # noqa: E402

from prism import core, origins  # noqa: E402
from prism.app import (  # noqa: E402
    App,
    Customer,
    CustomerConfigError,
    SingleCustomerCredentialsStore,
    fetch_original,
    process,
)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(TESTS_DIR, "images", "Phyciodes_mylitta_wide.jpeg"), "rb") as _f:
    JPEG = _f.read()
with open(os.path.join(TESTS_DIR, "images", "Phyciodes_mylitta_tall.jpeg"), "rb") as _f:
    OTHER_JPEG = _f.read()

# Kept before any test replaces it with a mock.
REAL_SENTRY_CAPTURE = sentry_sdk.capture_exception

PATH = "photos/5a1b2c3d4e5f60718293a4b5/0123abcd.jpg"
CDN = "https://cdn.example.net"


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def response(status=200, body=b"", headers=None, content_length=True) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = body
    r.headers = CaseInsensitiveDict(headers or {})
    if content_length and "Content-Length" not in r.headers:
        r.headers["Content-Length"] = str(len(body))
    return r


def image(data=JPEG, content_type="image/jpeg", etag=None, **kwargs) -> requests.Response:
    headers = {"Content-Type": content_type, "ETag": f'"{etag or md5_hex(data)}"'}
    return response(200, data, headers, **kwargs)


def s3_error(status, code) -> requests.Response:
    body = f'<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>m</Message></Error>'
    return response(status, body.encode(), {"Content-Type": "application/xml"})


def head(status=200, etag=None, length=None) -> requests.Response:
    headers = {}
    if etag is not None:
        headers["ETag"] = f'"{etag}"'
    if length is not None:
        headers["Content-Length"] = str(length)
    return response(status, b"", headers, content_length=False)


def origin_name(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.hostname == "cdn.example.net":
        return "cdn"
    if parsed.path.startswith("/primary/"):
        return "read"
    if parsed.path.startswith("/fallback/"):
        return "fallback"
    raise AssertionError(f"unexpected URL {url}")


@dataclass
class Call:
    method: str
    origin: str
    url: str
    timeout: Any
    kwargs: Dict[str, Any] = field(default_factory=dict)


class FakeHttp:
    """Stands in for the origin session.

    ``routes`` maps ``(method, origin)`` to a response, an exception to raise, or a list of
    those that is consumed one request at a time.
    """

    def __init__(self, routes):
        self.routes = dict(routes)
        self.calls: List[Call] = []
        self.lock = threading.Lock()

    def request(self, method, url, timeout=None, **kwargs):
        name = origin_name(url)
        with self.lock:
            self.calls.append(Call(method, name, url, timeout, kwargs))
            if (method, name) not in self.routes:
                raise AssertionError(f"unexpected {method} to {name}")
            spec = self.routes[(method, name)]
            if isinstance(spec, list):
                spec = spec.pop(0)
        if isinstance(spec, BaseException):
            raise spec
        return spec

    def called(self, method=None):
        return [(c.method, c.origin) for c in self.calls if method is None or c.method == method]


def make_customer(**overrides) -> Customer:
    config = dict(
        read_bucket_name="primary",
        read_bucket_region="N/A",
        read_bucket_endpoint_url="https://ceph.example.org",
        read_bucket_key_id="key",
        read_bucket_secret_key="secret",
        read_bucket_private=True,
        write_bucket_name="thumbs",
        fallback_bucket_name="fallback",
        fallback_bucket_region="us-east-1",
    )
    config.update(overrides)
    config = {k: v for k, v in config.items() if v is not None}
    return Customer(**config)


def legacy_customer(**overrides) -> Customer:
    """A customer entry written before the fallback settings existed."""
    config = dict(
        read_bucket_name="primary",
        read_bucket_region="N/A",
        read_bucket_endpoint_url="https://ceph.example.org",
        write_bucket_name="thumbs",
    )
    config.update(overrides)
    return Customer(**config)


class OriginTestCase(unittest.TestCase):
    def setUp(self):
        origins.STATS.reset()
        origins._sentry_last_sent.clear()
        patcher = mock.patch.object(origins.sentry_sdk, "capture_exception")
        self.sentry = patcher.start()
        self.addCleanup(patcher.stop)
        self.queue = origins.WriteBackQueue(workers=1, max_items=8, max_pending_bytes=10 * 1024 * 1024, autostart=False)
        patcher = mock.patch.object(origins, "default_write_back_queue", return_value=self.queue)
        patcher.start()
        self.addCleanup(patcher.stop)

    def use_http(self, routes) -> FakeHttp:
        self.http = FakeHttp(routes)
        patcher = mock.patch.object(origins, "_session", return_value=self.http)
        patcher.start()
        self.addCleanup(patcher.stop)
        return self.http

    def fetch(self, routes, customer=None):
        self.use_http(routes)
        return fetch_original(PATH, customer or make_customer())


# ---------------------------------------------------------------------------------------------
# Fallback rules
# ---------------------------------------------------------------------------------------------


class TestFallbackRules(OriginTestCase):
    def test_primary_hit_does_not_touch_fallback(self):
        im = self.fetch({("GET", "read"): image()})
        self.assertEqual((im.width, im.height), (500, 675))
        self.assertEqual(self.http.called(), [("GET", "read")])

    def test_primary_miss_is_served_from_fallback(self):
        im = self.fetch({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)
        self.assertEqual(self.http.called(), [("GET", "read"), ("GET", "fallback")])
        self.assertEqual(origins.STATS.snapshot()["served.fallback"], 1)

    def test_bytes_read_from_the_fallback_are_counted(self):
        self.use_http({("GET", "read"): [s3_error(404, "NoSuchKey")] * 2, ("GET", "fallback"): [image(), image(OTHER_JPEG)]})
        fetch_original(PATH, make_customer())
        fetch_original(PATH, make_customer())
        self.assertEqual(origins.STATS.snapshot()["fallback.bytes"], len(JPEG) + len(OTHER_JPEG))

    def test_bytes_read_from_the_primary_are_not_counted_as_fallback_bytes(self):
        self.fetch({("GET", "read"): image()})
        self.assertNotIn("fallback.bytes", origins.STATS.snapshot())

    def test_broken_fallback_reads_are_not_counted_as_fallback_bytes(self):
        with self.assertRaises(BadRequest):
            self.fetch({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image(b"junk")})
        self.assertNotIn("fallback.bytes", origins.STATS.snapshot())

    def test_fallback_bytes_appear_in_the_stats_log_line(self):
        self.fetch({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image()})
        with self.assertLogs("prism.origins", level="INFO") as logs:
            origins.STATS.log_now()
        self.assertIn(f"fallback.bytes={len(JPEG)}", logs.output[-1])

    def test_primary_miss_logs_the_s3_error_code(self):
        with self.assertLogs("prism.origins", level="INFO") as logs:
            self.fetch({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image()})
        self.assertTrue(any("missing" in line and "404 NoSuchKey" in line for line in logs.output), logs.output)
        self.assertTrue(any("served by origin=fallback" in line for line in logs.output), logs.output)

    def test_primary_empty_file_is_served_from_fallback(self):
        im = self.fetch({("GET", "read"): image(b""), ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)

    def test_primary_undecodable_file_is_served_from_fallback(self):
        im = self.fetch({("GET", "read"): image(b"not an image"), ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)

    def test_primary_short_read_is_served_from_fallback(self):
        short = image(JPEG[:1000])
        short.headers["Content-Length"] = str(len(JPEG))
        im = self.fetch({("GET", "read"): short, ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)

    def test_missing_everywhere_is_not_found(self):
        # A public AWS bucket answers 403 AccessDenied for a missing key to anonymous callers.
        with self.assertRaises(NotFound):
            self.fetch({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): s3_error(403, "AccessDenied")})
        self.assertEqual(self.http.called(), [("GET", "read"), ("GET", "fallback")])

    def test_broken_primary_and_missing_fallback_is_bad_request_with_reason(self):
        with self.assertRaises(BadRequest) as ctx:
            self.fetch({("GET", "read"): image(b""), ("GET", "fallback"): s3_error(403, "AccessDenied")})
        self.assertEqual(ctx.exception.description, core.EmptyOriginalFile.message)

    def test_undecodable_everywhere_is_bad_request(self):
        with self.assertRaises(BadRequest) as ctx:
            self.fetch({("GET", "read"): image(b"junk"), ("GET", "fallback"): image(b"junk")})
        self.assertEqual(ctx.exception.description, core.InvalidImageError.message)

    def test_primary_server_error_falls_back_loudly(self):
        with self.assertLogs("prism.origins", level="WARNING") as logs:
            im = self.fetch({("GET", "read"): s3_error(503, "SlowDown"), ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)
        self.assertTrue(any("WARNING" in line and "unavailable" in line and "503 SlowDown" in line for line in logs.output))
        self.sentry.assert_called_once()
        self.assertIsInstance(self.sentry.call_args.args[0], origins.OriginUnavailable)

    def test_primary_connection_error_falls_back_loudly(self):
        error = requests.ConnectionError("HTTPSConnectionPool(host='ceph.example.org', port=443): Max retries exceeded")
        with self.assertLogs("prism.origins", level="WARNING"):
            im = self.fetch({("GET", "read"): error, ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)
        self.sentry.assert_called_once()

    def test_primary_timeout_falls_back(self):
        with self.assertLogs("prism.origins", level="WARNING"):
            im = self.fetch({("GET", "read"): requests.ConnectTimeout("timed out"), ("GET", "fallback"): image()})
        self.assertEqual(im.width, 500)

    def test_sentry_events_for_an_outage_are_throttled(self):
        self.use_http({("GET", "read"): [s3_error(503, "x"), s3_error(503, "x")], ("GET", "fallback"): [image(), image()]})
        with self.assertLogs("prism.origins", level="WARNING") as logs:
            fetch_original(PATH, make_customer())
            fetch_original(PATH, make_customer())
        self.assertEqual(len([line for line in logs.output if "unavailable" in line]), 2)
        self.assertEqual(self.sentry.call_count, 1)

    def test_primary_unavailable_and_fallback_missing_is_bad_gateway(self):
        with self.assertLogs("prism.origins", level="WARNING"):
            with self.assertRaises(BadGateway) as ctx:
                self.fetch({("GET", "read"): requests.ConnectionError("down"), ("GET", "fallback"): s3_error(403, "AccessDenied")})
        self.assertIn("read origin is unavailable", ctx.exception.description)

    def test_private_primary_refusing_credentials_does_not_fall_back(self):
        with self.assertLogs("prism.origins", level="ERROR") as logs:
            with self.assertRaises(BadGateway) as ctx:
                self.fetch({("GET", "read"): s3_error(403, "SignatureDoesNotMatch"), ("GET", "fallback"): image()})
        self.assertEqual(self.http.called(), [("GET", "read")])
        self.assertIn("403 SignatureDoesNotMatch", ctx.exception.description)
        self.assertTrue(any("misconfigured" in line for line in logs.output))
        self.sentry.assert_called_once()

    def test_private_primary_access_denied_does_not_fall_back(self):
        with self.assertLogs("prism.origins", level="ERROR"):
            with self.assertRaises(BadGateway):
                self.fetch({("GET", "read"): s3_error(403, "AccessDenied"), ("GET", "fallback"): image()})
        self.assertEqual(self.http.called(), [("GET", "read")])

    def test_missing_bucket_is_an_error_not_a_miss(self):
        with self.assertLogs("prism.origins", level="ERROR"):
            with self.assertRaises(BadGateway) as ctx:
                self.fetch({("GET", "read"): s3_error(404, "NoSuchBucket"), ("GET", "fallback"): image()})
        self.assertEqual(self.http.called(), [("GET", "read")])
        self.assertIn("NoSuchBucket", ctx.exception.description)

    def test_redirect_from_the_primary_is_an_error_not_a_broken_copy(self):
        # S3 answers a request sent to the wrong regional endpoint with 301 PermanentRedirect.
        with self.assertLogs("prism.origins", level="ERROR"):
            with self.assertRaises(BadGateway) as ctx:
                self.fetch({("GET", "read"): s3_error(301, "PermanentRedirect"), ("GET", "fallback"): image()})
        self.assertIn("301 PermanentRedirect", ctx.exception.description)
        self.assertEqual(self.http.called(), [("GET", "read")])

    def test_public_primary_403_is_a_miss(self):
        im = self.fetch(
            {("GET", "read"): s3_error(403, "AccessDenied"), ("GET", "fallback"): image()},
            make_customer(read_bucket_private=False),
        )
        self.assertEqual(im.width, 500)

    def test_fallback_server_error_after_a_miss_is_bad_gateway(self):
        with self.assertLogs("prism.origins", level="WARNING"):
            with self.assertRaises(BadGateway):
                self.fetch({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): s3_error(500, "InternalError")})

    def test_without_a_fallback_a_miss_is_not_found(self):
        with self.assertRaises(NotFound):
            self.fetch({("GET", "read"): s3_error(404, "NoSuchKey")}, make_customer(fallback_bucket_name=None))
        self.assertEqual(self.http.called(), [("GET", "read")])

    def test_origin_requests_use_the_short_timeout_and_ask_for_unencoded_bytes(self):
        self.fetch({("GET", "read"): image()})
        call = self.http.calls[0]
        self.assertEqual(call.timeout, (3.0, 5.0))
        self.assertEqual(call.kwargs["headers"]["Accept-Encoding"], "identity")


class TestRetryBudget(unittest.TestCase):
    def test_session_retries_once_on_errors_and_5xx(self):
        session = origins._session()
        for scheme in ("https://", "http://"):
            retry = session.get_adapter(scheme + "example.org").max_retries
            self.assertEqual(retry.total, 1)
            self.assertIn(503, retry.status_forcelist)
            self.assertFalse(retry.raise_on_status)

    def test_session_is_per_thread(self):
        sessions = []
        thread = threading.Thread(target=lambda: sessions.append(origins._session()))
        thread.start()
        thread.join()
        self.assertIsNot(sessions[0], origins._session())


# ---------------------------------------------------------------------------------------------
# CDN fallback
# ---------------------------------------------------------------------------------------------


class TestCdnFallback(OriginTestCase):
    def cdn_customer(self, **overrides):
        return make_customer(fallback_bucket_name=None, fallback_bucket_region=None, fallback_cdn_url=CDN + "/", **overrides)

    def test_cdn_url_is_the_base_url_plus_the_key(self):
        cdn = self.cdn_customer().origins()[1]
        self.assertIsInstance(cdn, origins.HttpOrigin)
        self.assertEqual(cdn.url(PATH), f"{CDN}/{PATH}")
        self.assertEqual(cdn.url("/" + PATH, method="HEAD"), f"{CDN}/{PATH}")
        self.assertFalse(cdn.private)

    def test_cdn_url_percent_encodes_the_key(self):
        cdn = origins.HttpOrigin("fallback", CDN)
        self.assertEqual(cdn.url("photos/a b+c/d.jpg"), f"{CDN}/photos/a%20b%2Bc/d.jpg")

    def test_primary_miss_is_served_by_the_cdn_without_a_signature(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "cdn"): image()})
        im = fetch_original(PATH, self.cdn_customer())
        self.assertEqual(im.width, 500)
        cdn_call = self.http.calls[1]
        self.assertEqual(cdn_call.url, f"{CDN}/{PATH}")
        self.assertNotIn("Signature", cdn_call.url)
        self.assertEqual(origins.STATS.snapshot()["fallback.bytes"], len(JPEG))

    def test_cdn_403_is_a_miss(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "cdn"): s3_error(403, "AccessDenied")})
        with self.assertLogs("prism.origins", level="WARNING"):
            with self.assertRaises(NotFound):
                fetch_original(PATH, self.cdn_customer())

    def cdn_logs(self, cdn_answer, error=NotFound):
        """Fetch through a read-bucket miss and a CDN answering ``cdn_answer``; return the logs."""
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "cdn"): cdn_answer})
        with self.assertLogs("prism.origins", level="INFO") as logs:
            with self.assertRaises(error):
                fetch_original(PATH, self.cdn_customer())
        return logs.output

    def test_cdn_403_is_logged_as_a_warning_with_the_status_and_body(self):
        # A CDN answers a missing key with 404, so a 403 means an origin policy, a firewall rule
        # or a distribution error: the client still gets a 404, but the operator must see it.
        page = "<html>\n  <head><title>403 Forbidden</title></head>\n  <body>Request blocked. " + "x" * 400 + "</body></html>"
        logs = self.cdn_logs(response(403, page.encode(), {"Content-Type": "text/html"}))
        warning, = [line for line in logs if line.startswith("WARNING")]
        self.assertIn("reason=403: body '<html> <head><title>403 Forbidden</title></head> <body>Request blocked.", warning)
        excerpt = warning.split("body '", 1)[1].split("'", 1)[0]
        self.assertEqual(len(excerpt), origins.BODY_EXCERPT_CHARS)
        self.assertNotIn("\n", warning)

    def test_cdn_404_is_a_quiet_miss(self):
        logs = self.cdn_logs(response(404, b"<html>Not Found</html>"))
        self.assertEqual([line for line in logs if line.startswith("WARNING")], [])
        self.assertTrue(any(line.startswith("INFO") and "missing" in line and "reason=404 " in line for line in logs), logs)

    def test_cdn_server_error_warning_includes_the_body(self):
        logs = self.cdn_logs(response(503, b"Service Unavailable: origin timed out"), error=BadGateway)
        self.assertTrue(any(
            line.startswith("WARNING") and "503: body 'Service Unavailable: origin timed out'" in line for line in logs
        ), logs)

    def test_cdn_error_body_is_scrubbed_of_signatures(self):
        body = b"AccessDenied for /photos/a.jpg?X-Amz-Signature=deadbeef&X-Amz-Credential=AKIDEXAMPLE"
        logs = self.cdn_logs(response(403, body))
        warning, = [line for line in logs if line.startswith("WARNING")]
        self.assertNotIn("deadbeef", warning)
        self.assertNotIn("AKIDEXAMPLE", warning)

    def test_cdn_credentials_in_the_base_url_are_not_logged(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "cdn"): response(403, b"denied")})
        customer = make_customer(fallback_bucket_name=None, fallback_bucket_region=None,
                                 fallback_cdn_url="https://user:hunter2@cdn.example.net")
        with self.assertLogs("prism.origins", level="INFO") as logs:
            with self.assertRaises(NotFound):
                fetch_original(PATH, customer)
        self.assertFalse(any("hunter2" in line for line in logs.output), logs.output)
        self.assertTrue(any("url=https://cdn.example.net" in line for line in logs.output), logs.output)

    def test_cdn_403_to_a_head_request_is_a_warning(self):
        self.use_http({("HEAD", "read"): head(404), ("HEAD", "cdn"): head(403)})
        with self.assertLogs("prism.origins", level="WARNING") as logs:
            with self.assertRaises(origins.ReadFailed) as ctx:
                origins.locate_original(PATH, self.cdn_customer().origins())
        self.assertEqual(ctx.exception.status, 404)
        self.assertTrue(any("403" in line for line in logs.output), logs.output)

    def test_public_bucket_403_stays_a_quiet_miss(self):
        # Anonymous S3 callers get 403 AccessDenied for a missing key, so this is routine.
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): s3_error(403, "AccessDenied")})
        with self.assertLogs("prism.origins", level="INFO") as logs:
            with self.assertRaises(NotFound):
                fetch_original(PATH, make_customer())
        self.assertEqual([line for line in logs.output if line.startswith("WARNING")], [])

    def test_bucket_and_cdn_fallback_together_are_rejected(self):
        with self.assertRaises(CustomerConfigError):
            make_customer(fallback_cdn_url=CDN)

    def test_cdn_url_must_be_http(self):
        with self.assertRaises(CustomerConfigError):
            self.cdn_customer().__class__(read_bucket_name="primary", fallback_cdn_url="cdn.example.net")


# ---------------------------------------------------------------------------------------------
# Write-back
# ---------------------------------------------------------------------------------------------


class TestWriteBack(OriginTestCase):
    def wb_customer(self, **overrides):
        return make_customer(fallback_write_back=True, **overrides)

    def run_write_back(self, routes, customer=None):
        self.use_http(routes)
        im = fetch_original(PATH, customer or self.wb_customer())
        with self.assertLogs("prism.origins", level="INFO") as logs:
            self.queue.run_pending()
        self.logs = logs.output
        return im

    def puts(self):
        return [c for c in self.http.calls if c.method == "PUT"]

    def test_queued_job_holds_the_bytes_but_not_the_decoded_image(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image()})
        fetch_original(PATH, self.wb_customer())
        job = self.queue._queue.get_nowait()
        self.assertEqual(job.data, JPEG)
        self.assertFalse(any(hasattr(value, "width") for value in vars(job).values()))

    def test_disabled_by_default(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image()})
        fetch_original(PATH, make_customer())
        self.assertEqual(self.queue._queue.qsize(), 0)

    def test_written_when_primary_is_missing(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(content_type="image/pjpeg"),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): response(200, b"", {"ETag": f'"{md5_hex(JPEG)}"'}),
        })
        put, = self.puts()
        self.assertEqual(put.kwargs["data"], JPEG)
        self.assertEqual(put.kwargs["headers"]["Content-Type"], "image/pjpeg")
        self.assertEqual(put.kwargs["headers"]["Content-MD5"], base64.b64encode(hashlib.md5(JPEG).digest()).decode())
        parsed = urllib.parse.urlparse(put.url)
        self.assertEqual((parsed.hostname, parsed.path), ("ceph.example.org", "/primary/" + PATH))
        self.assertIn("Signature", urllib.parse.parse_qs(parsed.query))
        self.assertTrue(any("write-back written" in line for line in self.logs), self.logs)
        self.assertEqual(origins.STATS.snapshot()["write_back.written"], 1)

    def test_existing_valid_copy_is_not_overwritten(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(200, etag=md5_hex(JPEG), length=len(JPEG)),
        })
        self.assertEqual(self.puts(), [])
        self.assertTrue(any("write-back exists" in line for line in self.logs))

    def test_copy_that_appeared_after_a_miss_is_left_alone(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(200, etag=md5_hex(OTHER_JPEG), length=len(OTHER_JPEG)),
        })
        self.assertEqual(self.puts(), [])
        self.assertEqual(origins.STATS.snapshot()["write_back.exists"], 1)

    def test_empty_primary_copy_is_replaced(self):
        empty_etag = md5_hex(b"")
        self.run_write_back({
            ("GET", "read"): image(b""),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(200, etag=empty_etag, length=0),
            ("PUT", "read"): response(200, b"", {"ETag": f'"{md5_hex(JPEG)}"'}),
        })
        put, = self.puts()
        self.assertEqual(put.kwargs["data"], JPEG)
        self.assertTrue(any("write-back replaced-broken" in line for line in self.logs), self.logs)

    def test_undecodable_primary_copy_is_replaced(self):
        self.run_write_back({
            ("GET", "read"): image(b"garbage"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(200, etag=md5_hex(b"garbage"), length=7),
            ("PUT", "read"): response(200),
        })
        self.assertEqual(len(self.puts()), 1)
        self.assertEqual(origins.STATS.snapshot()["write_back.replaced-broken"], 1)

    def test_broken_copy_that_changed_since_the_read_is_left_alone(self):
        self.run_write_back({
            ("GET", "read"): image(b"garbage"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(200, etag=md5_hex(OTHER_JPEG), length=len(OTHER_JPEG)),
        })
        self.assertEqual(self.puts(), [])

    def test_undecodable_fallback_is_never_written(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image(b"junk")})
        with self.assertRaises(BadRequest):
            fetch_original(PATH, self.wb_customer())
        self.assertEqual(self.queue._queue.qsize(), 0)

    def test_fallback_bytes_not_matching_their_etag_are_skipped(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(etag=md5_hex(OTHER_JPEG)),
        })
        self.assertEqual(self.puts(), [])
        self.assertTrue(any("write-back skipped" in line and "ETag" in line for line in self.logs), self.logs)

    def test_fallback_without_content_length_is_skipped(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(content_length=False),
        })
        self.assertEqual(self.puts(), [])
        self.assertEqual(origins.STATS.snapshot()["write_back.skipped"], 1)

    def test_multipart_etag_is_not_compared(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(etag="0123456789abcdef0123456789abcdef-2"),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): response(200),
        })
        self.assertEqual(len(self.puts()), 1)

    def test_not_attempted_when_primary_was_unavailable(self):
        self.use_http({("GET", "read"): requests.ConnectionError("down"), ("GET", "fallback"): image()})
        with self.assertLogs("prism.origins", level="INFO"):
            fetch_original(PATH, self.wb_customer())
        self.assertEqual(self.queue._queue.qsize(), 0)
        self.assertEqual(origins.STATS.snapshot()["write_back.skipped"], 1)

    def test_not_attempted_when_primary_served(self):
        self.use_http({("GET", "read"): image()})
        fetch_original(PATH, self.wb_customer())
        self.assertEqual(self.queue._queue.qsize(), 0)

    def test_failed_put_is_logged_and_counted(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): s3_error(500, "InternalError"),
        })
        self.assertTrue(any("WARNING" in line and "write-back failed" in line and "PUT 500" in line for line in self.logs), self.logs)
        self.assertEqual(origins.STATS.snapshot()["write_back.failed"], 1)

    def test_put_connection_error_is_a_failure_without_signature_in_the_log(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): requests.ConnectionError("Max retries exceeded with url: /primary/x.jpg?Signature=abc%3D&Expires=1&AWSAccessKeyId=key"),
        })
        line, = [line for line in self.logs if "write-back failed" in line]
        self.assertIn("Signature=[redacted]", line)
        self.assertNotIn("abc%3D", line)

    def test_only_original_bytes_are_written_never_the_resized_output(self):
        self.use_http({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): response(200),
        })
        args = {
            "command": "resize",
            "options": {"w": 100, "h": 100, "q": 80, "out_format": "jpg", "premultiplied_alpha": None, "filters": None},
            "debug": True,
        }
        resized = process(PATH, args, self.wb_customer()).response.getvalue()
        self.assertNotEqual(resized, JPEG)
        with self.assertLogs("prism.origins", level="INFO"):
            self.queue.run_pending()
        put, = self.puts()
        self.assertEqual(put.kwargs["data"], JPEG)

    def test_full_queue_drops_and_logs(self):
        self.queue = origins.WriteBackQueue(workers=1, max_items=1, max_pending_bytes=10 * 1024 * 1024, autostart=False)
        self.use_http({
            ("GET", "read"): [s3_error(404, "NoSuchKey"), s3_error(404, "NoSuchKey")],
            ("GET", "fallback"): [image(), image()],
        })
        target = self.wb_customer().write_back_target()
        with self.assertLogs("prism.origins", level="WARNING") as logs:
            origins.read_original("a.jpg", self.wb_customer().origins(), write_back=target, write_back_queue=self.queue)
            im = origins.read_original("b.jpg", self.wb_customer().origins(), write_back=target, write_back_queue=self.queue)
        self.assertEqual(im.image.width, 500)
        self.assertTrue(any("write-back dropped" in line and "queue full" in line for line in logs.output))
        self.assertEqual(origins.STATS.snapshot()["write_back.dropped"], 1)

    def test_pending_bytes_limit_drops(self):
        queue = origins.WriteBackQueue(workers=1, max_items=10, max_pending_bytes=len(JPEG) + 10, autostart=False)
        target = self.wb_customer().write_back_target()
        self.assertTrue(queue.submit(origins.WriteBackJob(target, "a.jpg", JPEG, content_length=len(JPEG))))
        with self.assertLogs("prism.origins", level="WARNING"):
            self.assertFalse(queue.submit(origins.WriteBackJob(target, "b.jpg", JPEG, content_length=len(JPEG))))

    def test_a_key_already_queued_is_not_queued_twice(self):
        target = self.wb_customer().write_back_target()
        self.assertTrue(self.queue.submit(origins.WriteBackJob(target, PATH, JPEG, content_length=len(JPEG))))
        self.assertFalse(self.queue.submit(origins.WriteBackJob(target, PATH, JPEG, content_length=len(JPEG))))
        self.assertEqual(self.queue._queue.qsize(), 1)

    def test_queueing_error_does_not_affect_the_response(self):
        self.use_http({("GET", "read"): s3_error(404, "NoSuchKey"), ("GET", "fallback"): image()})
        with mock.patch.object(self.queue, "submit", side_effect=RuntimeError("boom")):
            with self.assertLogs("prism.origins", level="ERROR"):
                im = fetch_original(PATH, self.wb_customer())
        self.assertEqual(im.width, 500)

    def test_background_failure_does_not_affect_the_response(self):
        """With real worker threads, a PUT that fails leaves the served image untouched."""
        self.queue = origins.WriteBackQueue(workers=1, max_items=8, max_pending_bytes=10 * 1024 * 1024)
        self.use_http({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): requests.ConnectionError("down"),
        })
        with self.assertLogs("prism.origins", level="INFO"):
            im = origins.read_original(
                PATH, self.wb_customer().origins(), write_back=self.wb_customer().write_back_target(),
                write_back_queue=self.queue,
            )
            self.queue.join()
        self.assertEqual(im.image.width, 500)
        self.assertEqual(im.data, JPEG)
        self.assertEqual(origins.STATS.snapshot()["write_back.failed"], 1)

    def failing_jobs(self, routes, keys):
        """Run one write-back job per key against ``routes`` and return the log lines."""
        self.use_http(routes)
        target = self.wb_customer().write_back_target()
        for key in keys:
            self.queue.submit(origins.WriteBackJob(target, key, JPEG, content_length=len(JPEG)))
        with self.assertLogs("prism.origins", level="INFO") as logs:
            self.queue.run_pending()
        return logs.output

    def test_refused_put_is_reported_to_sentry(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): s3_error(403, "AccessDenied"),
        })
        self.assertTrue(any("WARNING" in line and "write-back failed" in line for line in self.logs), self.logs)
        self.assertEqual(origins.STATS.snapshot()["write_back.failed"], 1)
        self.sentry.assert_called_once()
        event = self.sentry.call_args.args[0]
        self.assertIsInstance(event, origins.WriteBackFailed)
        self.assertIn("PUT 403 AccessDenied", str(event))
        self.assertIn("bucket=primary", str(event))
        # The key is left out so that Sentry groups every failure of one kind together.
        self.assertNotIn(PATH, str(event))

    def test_refused_head_is_reported_to_sentry(self):
        self.failing_jobs({("HEAD", "read"): head(403)}, ["a.jpg"])
        self.sentry.assert_called_once()
        self.assertIn("HEAD 403", str(self.sentry.call_args.args[0]))

    def test_unreachable_read_bucket_during_write_back_is_reported_to_sentry(self):
        self.failing_jobs({("HEAD", "read"): requests.ConnectionError("down")}, ["a.jpg"])
        self.sentry.assert_called_once()
        self.assertIsInstance(self.sentry.call_args.args[0], origins.OriginUnavailable)

    def test_write_back_failures_are_reported_once_per_kind_per_interval(self):
        now = [1000.0]
        with mock.patch.object(origins.time, "monotonic", side_effect=lambda: now[0]):
            logs = self.failing_jobs({
                ("HEAD", "read"): [head(404), head(404), head(403)],
                ("PUT", "read"): [s3_error(403, "AccessDenied"), s3_error(403, "AccessDenied")],
            }, ["a.jpg", "b.jpg", "c.jpg"])
            # Every failure is still logged and counted; only the Sentry events are throttled.
            self.assertEqual(len([line for line in logs if "write-back failed" in line]), 3)
            self.assertEqual(origins.STATS.snapshot()["write_back.failed"], 3)
            reported = [str(call.args[0]) for call in self.sentry.call_args_list]
            self.assertEqual(len(reported), 2, reported)
            self.assertTrue(any("PUT 403 AccessDenied" in text for text in reported))
            self.assertTrue(any("HEAD 403" in text for text in reported))

            now[0] += origins.WRITE_BACK_SENTRY_MIN_INTERVAL - 1
            self.failing_jobs({("HEAD", "read"): head(404), ("PUT", "read"): s3_error(403, "AccessDenied")}, ["d.jpg"])
            self.assertEqual(self.sentry.call_count, 2)

            now[0] += 2
            self.failing_jobs({("HEAD", "read"): head(404), ("PUT", "read"): s3_error(403, "AccessDenied")}, ["e.jpg"])
            self.assertEqual(self.sentry.call_count, 3)

    def test_successful_write_back_is_not_reported(self):
        self.run_write_back({
            ("GET", "read"): s3_error(404, "NoSuchKey"),
            ("GET", "fallback"): image(),
            ("HEAD", "read"): head(404),
            ("PUT", "read"): response(200),
        })
        self.sentry.assert_not_called()

    def test_write_back_failure_without_sentry_configured_is_only_logged(self):
        # The real client: prism.app initialises Sentry without a DSN here, so capturing does nothing.
        with mock.patch.object(origins.sentry_sdk, "capture_exception", REAL_SENTRY_CAPTURE):
            logs = self.failing_jobs({("HEAD", "read"): head(403)}, ["a.jpg"])
        self.assertTrue(any("write-back failed" in line for line in logs), logs)
        self.assertEqual(origins.STATS.snapshot()["write_back.failed"], 1)

    def test_an_error_while_reporting_does_not_stop_write_back(self):
        self.sentry.side_effect = RuntimeError("sentry is broken")
        logs = self.failing_jobs({
            ("HEAD", "read"): [head(403), head(404)],
            ("PUT", "read"): response(200),
        }, ["a.jpg", "b.jpg"])
        self.assertTrue(any("write-back failed" in line for line in logs), logs)
        self.assertTrue(any("write-back written" in line for line in logs), logs)
        self.assertEqual(self.queue._queue.qsize(), 0)

    def test_put_signature_covers_content_type_and_md5(self):
        target = self.wb_customer().write_back_target()
        with mock.patch("boto.s3.connection.time.time", return_value=1_700_000_000):
            a = target.url(PATH, method="PUT", headers={"Content-Type": "image/jpeg", "Content-MD5": "a"})
            b = target.url(PATH, method="PUT", headers={"Content-Type": "image/png", "Content-MD5": "a"})
            c = target.url(PATH, method="PUT", headers={"Content-Type": "image/jpeg", "Content-MD5": "b"})
        signature = lambda url: urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["Signature"][0]  # noqa: E731
        self.assertEqual(len({signature(a), signature(b), signature(c)}), 3)


# ---------------------------------------------------------------------------------------------
# GIF passthrough
# ---------------------------------------------------------------------------------------------


def gif_request(path="anim/cat.gif"):
    env = EnvironBuilder(
        method="GET", base_url="http://prism.example.org", path="/" + path, query_string="w=100"
    ).get_environ()
    return Request(env)


def status_of(resp):
    """The status of what App.dispatch_request returned: a Response or an HTTPException."""
    return getattr(resp, "status_code", None) or resp.code


class TestGifPassthrough(OriginTestCase):
    GIF = "anim/cat.gif"

    def app(self, **overrides):
        config = dict(
            read_bucket_name="primary",
            read_bucket_region="N/A",
            read_bucket_endpoint_url="https://ceph.example.org",
            read_bucket_key_id="key",
            read_bucket_secret_key="secret",
            read_bucket_private=True,
            fallback_bucket_name="fallback",
            fallback_bucket_region="us-east-1",
        )
        config.update(overrides)
        return App(credentials_store=SingleCustomerCredentialsStore(config))

    def test_missing_on_primary_redirects_to_fallback(self):
        self.use_http({("HEAD", "read"): head(404), ("HEAD", "fallback"): head(200, length=10)})
        resp = self.app().dispatch_request(gif_request(self.GIF))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.headers["Location"], f"https://s3.amazonaws.com/fallback/{self.GIF}")
        self.assertNotIn("Cache-Control", resp.headers)

    def test_private_primary_redirect_is_signed_and_not_stored(self):
        self.use_http({("HEAD", "read"): head(200, length=10)})
        resp = self.app().dispatch_request(gif_request(self.GIF))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("Signature=", resp.headers["Location"])
        self.assertEqual(resp.headers["Cache-Control"], "no-store")

    def test_private_primary_403_is_bad_gateway_not_fallback(self):
        self.use_http({("HEAD", "read"): head(403)})
        with self.assertLogs("prism.origins", level="ERROR"):
            resp = self.app().dispatch_request(gif_request(self.GIF))
        self.assertEqual(status_of(resp), 502)
        self.assertEqual(self.http.called(), [("HEAD", "read")])

    def test_unreachable_primary_falls_back(self):
        self.use_http({("HEAD", "read"): requests.ConnectionError("down"), ("HEAD", "fallback"): head(200, length=10)})
        with self.assertLogs("prism.origins", level="WARNING"):
            resp = self.app().dispatch_request(gif_request(self.GIF))
        self.assertEqual(resp.status_code, 302)

    def test_empty_primary_falls_back(self):
        self.use_http({("HEAD", "read"): head(200, length=0), ("HEAD", "fallback"): head(200, length=10)})
        resp = self.app().dispatch_request(gif_request(self.GIF))
        self.assertTrue(resp.headers["Location"].startswith("https://s3.amazonaws.com/fallback/"))

    def test_missing_everywhere_is_not_found(self):
        self.use_http({("HEAD", "read"): head(404), ("HEAD", "fallback"): head(403)})
        resp = self.app().dispatch_request(gif_request(self.GIF))
        self.assertEqual(status_of(resp), 404)

    def test_cdn_fallback_redirects_to_the_cdn(self):
        self.use_http({("HEAD", "read"): head(404), ("HEAD", "cdn"): head(200, length=10)})
        resp = self.app(fallback_bucket_name=None, fallback_cdn_url=CDN).dispatch_request(gif_request(self.GIF))
        self.assertEqual(resp.headers["Location"], f"{CDN}/{self.GIF}")


# ---------------------------------------------------------------------------------------------
# Configuration and backward compatibility
# ---------------------------------------------------------------------------------------------


class TestBackwardCompatibility(OriginTestCase):
    """A customer entry without any of the new settings behaves as it did before them."""

    def test_defaults(self):
        customer = legacy_customer()
        self.assertFalse(customer.read_bucket_private)
        self.assertFalse(customer.fallback_bucket_private)
        self.assertFalse(customer.fallback_write_back)
        self.assertIsNone(customer.write_back_target())
        only, = customer.origins()
        self.assertEqual(only.name, "read")
        self.assertFalse(only.private)
        self.assertEqual(only.url(PATH), f"https://ceph.example.org/primary/{PATH}")

    def test_write_bucket_defaults_are_unchanged(self):
        customer = Customer(read_bucket_name="originals", read_bucket_region="us-east-1")
        self.assertEqual(customer.write_bucket_name, "originals")
        self.assertEqual(customer.origins()[0].url(PATH), f"https://s3.amazonaws.com/originals/{PATH}")

    def test_hit(self):
        im = self.fetch({("GET", "read"): image()}, legacy_customer())
        self.assertEqual(im.width, 500)
        self.assertNotIn("Signature", self.http.calls[0].url)

    def test_404_and_public_403_are_not_found(self):
        for error in (s3_error(404, "NoSuchKey"), s3_error(403, "AccessDenied")):
            with self.subTest(status=error.status_code):
                with self.assertRaises(NotFound):
                    self.fetch({("GET", "read"): error}, legacy_customer())

    def test_empty_and_invalid_originals_are_bad_request_with_the_old_messages(self):
        cases = [(b"", core.EmptyOriginalFile.message), (b"junk", core.InvalidImageError.message)]
        for body, message in cases:
            with self.subTest(body=body):
                with self.assertRaises(BadRequest) as ctx:
                    self.fetch({("GET", "read"): image(body)}, legacy_customer())
                self.assertEqual(ctx.exception.description, message)

    def test_server_error_is_bad_gateway(self):
        # Before the origin rules this surfaced as an unhandled HTTPError (500).
        with self.assertLogs("prism.origins", level="WARNING"):
            with self.assertRaises(BadGateway):
                self.fetch({("GET", "read"): s3_error(500, "InternalError")}, legacy_customer())


class TestCustomerConfig(unittest.TestCase):
    def test_private_requires_both_keys(self):
        with self.assertRaises(CustomerConfigError):
            Customer(read_bucket_name="b", read_bucket_private=True, read_bucket_key_id="k")
        with self.assertRaises(CustomerConfigError):
            Customer(read_bucket_name="b", fallback_bucket_name="f", fallback_bucket_private=True, fallback_bucket_secret_key="s")

    def test_private_flags_are_coerced_to_bool(self):
        customer = Customer(read_bucket_name="b", read_bucket_private="true", read_bucket_key_id="k", read_bucket_secret_key="s")
        self.assertIs(customer.read_bucket_private, True)
        self.assertIs(Customer(read_bucket_name="b", read_bucket_private="false").read_bucket_private, False)
        self.assertIs(Customer(read_bucket_name="b", read_bucket_private=0).read_bucket_private, False)
        with self.assertRaises(CustomerConfigError):
            Customer(read_bucket_name="b", read_bucket_private="maybe")

    def test_write_back_requires_a_fallback_and_read_keys(self):
        with self.assertRaises(CustomerConfigError):
            Customer(read_bucket_name="b", read_bucket_key_id="k", read_bucket_secret_key="s", fallback_write_back=True)
        with self.assertRaises(CustomerConfigError):
            Customer(read_bucket_name="b", fallback_cdn_url=CDN, fallback_write_back=True)
        customer = Customer(read_bucket_name="b", read_bucket_key_id="k", read_bucket_secret_key="s",
                            fallback_cdn_url=CDN, fallback_write_back="true")
        target = customer.write_back_target()
        self.assertEqual((target.bucket_name, target.private), ("b", True))

    def test_invalid_config_answers_500_and_is_reported(self):
        app = App(credentials_store=SingleCustomerCredentialsStore({"read_bucket_name": "b", "read_bucket_private": True}))
        with mock.patch("prism.app.sentry_sdk.capture_exception") as capture:
            with self.assertLogs("prism.app", level="ERROR"):
                with self.assertRaises(InternalServerError):
                    app.get_customer(gif_request())
        capture.assert_called_once()


# ---------------------------------------------------------------------------------------------
# Signed URLs, logging and Sentry
# ---------------------------------------------------------------------------------------------

SIGNED = "https://ceph.example.org/primary/a.jpg?Signature=abc%2Bdef%3D&Expires=1700000000&AWSAccessKeyId=AKIAEXAMPLE"


class TestSignedUrlScrubbing(unittest.TestCase):
    def test_scrub_redacts_signature_parameters(self):
        text = f"Max retries exceeded with url: {SIGNED} (Caused by x)"
        scrubbed = origins.scrub(text)
        self.assertNotIn("abc%2Bdef", scrubbed)
        self.assertNotIn("AKIAEXAMPLE", scrubbed)
        self.assertIn("Signature=[redacted]", scrubbed)
        self.assertIn("Expires=1700000000", scrubbed)
        self.assertIn("/primary/a.jpg", scrubbed)
        v4 = "https://s3.amazonaws.com/b/k?X-Amz-Credential=AKIA%2F2026&X-Amz-Signature=deadbeef&X-Amz-Date=1"
        self.assertNotIn("deadbeef", origins.scrub(v4))
        self.assertNotIn("AKIA%2F2026", origins.scrub(v4))

    def test_urllib3_retry_warnings_are_scrubbed(self):
        origins.install_log_scrubbing()
        with self.assertLogs("urllib3.connectionpool", level="WARNING") as logs:
            logging.getLogger("urllib3.connectionpool").warning(
                "Retrying (%r) after connection broken by '%r': %s", "Retry(total=0)", "err", SIGNED.split("ceph.example.org")[1]
            )
        self.assertNotIn("abc%2Bdef", logs.output[0])
        self.assertIn("Signature=[redacted]", logs.output[0])

    def test_sentry_hooks_scrub_nested_values(self):
        event = {
            "exception": {"values": [{"value": f"404 Client Error for url: {SIGNED}"}]},
            "frames": [{"vars": {"url": f"'{SIGNED}'"}}],
            "tuple": (SIGNED,),
        }
        scrubbed = origins.sentry_before_send(event, {})
        self.assertNotIn("abc%2Bdef", repr(scrubbed))
        crumb = origins.sentry_before_breadcrumb({"data": {"url": SIGNED}}, {})
        self.assertNotIn("abc%2Bdef", repr(crumb))

    def test_origin_error_messages_are_scrubbed(self):
        origin = make_customer().origins()[0]
        error = origins.OriginUnavailable(origin, f"ConnectionError: Max retries exceeded with url: {SIGNED}")
        self.assertNotIn("abc%2Bdef", str(error))
        self.assertNotIn("abc%2Bdef", error.reason)

    def test_s3_config_repr_hides_the_secret(self):
        config = core.S3ConnectionConfig(key_id="k", secret_key="very-secret", region="r", endpoint_url="e")
        self.assertNotIn("very-secret", repr(config))
        self.assertNotIn("secret", repr(make_customer().origins()[0]))


class TestOriginUrls(unittest.TestCase):
    def test_private_origin_is_signed_and_public_origin_is_plain(self):
        primary, fallback = make_customer().origins()
        signed = urllib.parse.urlparse(primary.url(PATH))
        self.assertEqual(signed.hostname, "ceph.example.org")
        self.assertEqual(signed.path, "/primary/" + PATH)
        self.assertIn("Signature", urllib.parse.parse_qs(signed.query))
        self.assertEqual(fallback.url(PATH), "https://s3.amazonaws.com/fallback/" + PATH)

    def test_head_requests_get_their_own_signature(self):
        primary = make_customer().origins()[0]
        signature = lambda url: urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["Signature"]  # noqa: E731
        with mock.patch("boto.s3.connection.time.time", return_value=1_700_000_000):
            self.assertNotEqual(signature(primary.url(PATH)), signature(primary.url(PATH, method="HEAD")))


class TestStats(unittest.TestCase):
    def test_counters_are_logged_periodically_on_the_origins_logger(self):
        stats = origins.OriginStats(interval=0)
        with self.assertLogs("prism.origins", level="INFO") as logs:
            stats.incr("served.fallback")
            stats.incr("write_back.written")
        self.assertIn("served.fallback=1", logs.output[-1])
        self.assertIn("write_back.written=1", logs.output[-1])

    def test_counters_are_not_logged_before_the_interval(self):
        stats = origins.OriginStats(interval=3600)
        logger = logging.getLogger("prism.origins")
        with mock.patch.object(logger, "info") as info:
            stats.incr("served.read")
        info.assert_not_called()
        self.assertEqual(stats.snapshot(), {"served.read": 1})


# ---------------------------------------------------------------------------------------------
# Real HTTP: retry budget against a closed port, CDN fallback served by a local server
# ---------------------------------------------------------------------------------------------


class _ImageHandler(http.server.BaseHTTPRequestHandler):
    requests_seen: List[str] = []

    def do_GET(self):
        type(self).requests_seen.append(self.path)
        self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(JPEG)))
        self.send_header("ETag", f'"{md5_hex(JPEG)}"')
        self.end_headers()
        self.wfile.write(JPEG)

    def log_message(self, *args):
        pass


class TestRealHttp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ImageHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.cdn = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        _ImageHandler.requests_seen = []
        origins._sentry_last_sent.clear()
        patcher = mock.patch.object(origins.sentry_sdk, "capture_exception")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_unreachable_primary_falls_back_quickly_without_leaking_the_signature(self):
        customer = Customer(
            read_bucket_name="primary",
            read_bucket_region="N/A",
            read_bucket_endpoint_url="http://127.0.0.1:1",  # nothing listens here
            read_bucket_key_id="key",
            read_bucket_secret_key="secret",
            read_bucket_private=True,
            fallback_cdn_url=self.cdn,
        )
        started = time.monotonic()
        with self.assertLogs("prism.origins", level="WARNING") as logs:
            im = fetch_original(PATH, customer)
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertEqual(im.width, 500)
        self.assertEqual(_ImageHandler.requests_seen, ["/" + PATH])
        warning, = [line for line in logs.output if "unavailable" in line]
        self.assertIn("ConnectionError", warning)
        self.assertNotIn("Signature=", warning.replace("Signature=[redacted]", ""))


if __name__ == "__main__":
    unittest.main()
