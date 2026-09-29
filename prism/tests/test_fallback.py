"""Tests for reading originals from a fallback origin.

HTTP is stubbed: origins._session is replaced with FakeHttp, which answers per origin and
records every request, so these tests run without S3, minio or the network. The two tests in
TestRealHttp use a local HTTP server and a closed local port instead, to exercise the real
retry adapter and timeouts.
"""
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

from prism import core, origins  # noqa: E402
from prism.app import (  # noqa: E402
    App,
    Customer,
    CustomerConfigError,
    SingleCustomerCredentialsStore,
    fetch_original,
)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(TESTS_DIR, "images", "Phyciodes_mylitta_wide.jpeg"), "rb") as _f:
    JPEG = _f.read()
with open(os.path.join(TESTS_DIR, "images", "Phyciodes_mylitta_tall.jpeg"), "rb") as _f:
    OTHER_JPEG = _f.read()

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


# ---------------------------------------------------------------------------------------------
# Configuration and backward compatibility
# ---------------------------------------------------------------------------------------------


class TestBackwardCompatibility(OriginTestCase):
    """A customer entry without any of the new settings behaves as it did before them."""

    def test_defaults(self):
        customer = legacy_customer()
        self.assertFalse(customer.read_bucket_private)
        self.assertFalse(customer.fallback_bucket_private)
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
# Real HTTP: retry budget against a closed port, fallback served by a local server
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
            fallback_bucket_name="fallback",
            fallback_bucket_endpoint_url=self.cdn,
        )
        started = time.monotonic()
        with self.assertLogs("prism.origins", level="WARNING") as logs:
            im = fetch_original(PATH, customer)
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertEqual(im.width, 500)
        self.assertEqual(_ImageHandler.requests_seen, ["/fallback/" + PATH])
        warning, = [line for line in logs.output if "unavailable" in line]
        self.assertIn("ConnectionError", warning)
        self.assertNotIn("Signature=", warning.replace("Signature=[redacted]", ""))


if __name__ == "__main__":
    unittest.main()
