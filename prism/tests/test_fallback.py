"""Tests for reading originals from a fallback bucket when the read bucket lacks them.

These tests stub out the network: core.fetch_image is replaced with a fake that answers per
bucket, so they run without S3 or minio.
"""
import os
import unittest
import urllib.parse
from unittest import mock

import requests
from werkzeug.exceptions import BadRequest, NotFound

# prism.app builds a credentials store at import time and needs one of these set.
os.environ.setdefault("S3_BUCKET", "prism-test")

from prism import core  # noqa: E402
from prism.app import Customer, fetch_original  # noqa: E402

PATH = "photos/abc/def.jpg"


def http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


def make_customer(**overrides):
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
    return Customer(**config)


def fake_fetch(results):
    """Build a fetch_image replacement. `results` maps bucket name to an image or an exception."""
    calls = []

    def fetch(url):
        bucket = "primary" if "/primary/" in url else "fallback"
        calls.append(bucket)
        result = results[bucket]
        if isinstance(result, Exception):
            raise result
        return result

    return fetch, calls


class TestFetchOriginal(unittest.TestCase):
    def fetch(self, results, customer=None):
        fetch, calls = fake_fetch(results)
        with mock.patch.object(core, "fetch_image", side_effect=fetch):
            try:
                return fetch_original(PATH, customer or make_customer()), calls
            finally:
                self.calls = calls

    def test_primary_hit_does_not_touch_fallback(self):
        im, calls = self.fetch({"primary": "primary-image", "fallback": "fallback-image"})
        self.assertEqual(im, "primary-image")
        self.assertEqual(calls, ["primary"])

    def test_primary_miss_is_served_from_fallback(self):
        im, calls = self.fetch({"primary": http_error(404), "fallback": "fallback-image"})
        self.assertEqual(im, "fallback-image")
        self.assertEqual(calls, ["primary", "fallback"])

    def test_primary_empty_file_is_served_from_fallback(self):
        im, _ = self.fetch({"primary": core.EmptyOriginalFile(), "fallback": "fallback-image"})
        self.assertEqual(im, "fallback-image")

    def test_primary_invalid_image_is_served_from_fallback(self):
        im, _ = self.fetch({"primary": core.InvalidImageError(), "fallback": "fallback-image"})
        self.assertEqual(im, "fallback-image")

    def test_both_missing_is_not_found(self):
        # A public AWS bucket answers 403 for a missing key to anonymous callers.
        with self.assertRaises(NotFound):
            self.fetch({"primary": http_error(404), "fallback": http_error(403)})
        self.assertEqual(self.calls, ["primary", "fallback"])

    def test_empty_file_everywhere_is_bad_request(self):
        with self.assertRaises(BadRequest):
            self.fetch({"primary": core.EmptyOriginalFile(), "fallback": core.EmptyOriginalFile()})

    def test_primary_server_error_does_not_fall_back(self):
        with self.assertRaises(requests.HTTPError):
            self.fetch({"primary": http_error(503), "fallback": "fallback-image"})
        self.assertEqual(self.calls, ["primary"])

    def test_private_primary_refusing_credentials_does_not_fall_back(self):
        with self.assertRaises(requests.HTTPError):
            self.fetch({"primary": http_error(403), "fallback": "fallback-image"})
        self.assertEqual(self.calls, ["primary"])

    def test_public_primary_403_is_a_miss(self):
        customer = make_customer(read_bucket_private=False)
        im, calls = self.fetch({"primary": http_error(403), "fallback": "fallback-image"}, customer)
        self.assertEqual(im, "fallback-image")

    def test_without_fallback_a_miss_is_not_found(self):
        customer = make_customer(fallback_bucket_name=None)
        with self.assertRaises(NotFound):
            self.fetch({"primary": http_error(404)}, customer)
        self.assertEqual(self.calls, ["primary"])


class TestOriginUrls(unittest.TestCase):
    def test_private_origin_is_signed_and_public_origin_is_plain(self):
        primary, fallback = make_customer().origins()
        signed = urllib.parse.urlparse(primary.url(PATH))
        self.assertEqual(signed.hostname, "ceph.example.org")
        self.assertEqual(signed.path, "/primary/" + PATH)
        self.assertIn("Signature", urllib.parse.parse_qs(signed.query))
        self.assertEqual(fallback.url(PATH), "https://s3.amazonaws.com/fallback/" + PATH)

    def test_write_bucket_defaults_are_unchanged(self):
        customer = Customer(read_bucket_name="originals", read_bucket_region="us-east-1")
        self.assertEqual(customer.write_bucket_name, "originals")
        self.assertEqual([o.name for o in customer.origins()], ["read"])
        self.assertFalse(customer.origins()[0].private)


if __name__ == "__main__":
    unittest.main()
