"""Read original images from an ordered list of origins, and copy fallback reads back.

A customer normally has one origin: its read bucket. During a storage migration it can also
have a fallback origin (a second bucket, or an HTTPS CDN in front of one) that holds the
originals which have not been copied to the read bucket yet. This module decides, for each
original, which origin serves it:

* **Missing** on an origin (404, or 403 from a public origin, which is how S3 answers anonymous
  callers for a missing key) or **broken** there (empty, shorter than its Content-Length, or not
  decodable as an image): try the next origin quietly, logging the reason at INFO. The exception
  is a 403 from an HTTP origin (a CDN), which answers a missing key with 404: it is still a miss,
  but logged at WARNING with the start of the response body, since it usually means an origin
  policy, a firewall rule or an error page.
* **Unavailable** (connection error, timeout, 429 or 5xx): try the next origin, but loudly, with
  a WARNING and a Sentry event, because every such request costs a fallback read.
* **Misconfigured** (NoSuchBucket, a 403 from a private origin, a redirect, or any other 4xx): stop and
  return 502 without falling back, logged at ERROR and sent to Sentry. A wrong bucket name or
  key must not quietly move all traffic to the fallback.

Decoding catches empty and unreadable files but not every damaged one: ImageMagick decodes a
JPEG that is cut short (the missing rows come out grey), so a truncated copy on the read
bucket is served as it is. Nothing here verifies checksums on the read path.

When ``fallback_write_back`` is enabled for a customer, an original that was served by the
fallback is copied, byte for byte, into the read bucket under the same key by a small pool of
background threads, so the next request for it is served without another fallback read. See
``WriteBackQueue``.

All origin logging goes to the ``prism.origins`` logger, which can be enabled on its own with
the ``ORIGINS_LOG_LEVEL`` setting.
"""
import base64
import collections
import hashlib
import logging
import os
import queue
import re
import threading
import time
import urllib.parse
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
import sentry_sdk
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from wand.image import Image

from prism import core, settings

logger = logging.getLogger("prism.origins")


# ---------------------------------------------------------------------------------------------
# Keeping signed URLs out of logs and Sentry
# ---------------------------------------------------------------------------------------------

# Query parameters that make a signed URL usable by whoever reads it, plus the access key id.
_SIGNED_PARAM_RE = re.compile(
    r"((?:X-Amz-Signature|X-Amz-Credential|X-Amz-Security-Token|Signature|AWSAccessKeyId)=)[^&\s'\"<>)]*",
    re.IGNORECASE,
)


def scrub(text: str) -> str:
    """Replace the values of signature and access-key query parameters with ``[redacted]``."""
    if not text:
        return text
    return _SIGNED_PARAM_RE.sub(r"\1[redacted]", text)


class ScrubSignedUrlsFilter(logging.Filter):
    """Logging filter that redacts signed-URL parameters from a record's message and traceback.

    urllib3 logs the full request URL, query string included, when it retries a request, and
    requests puts the URL into its exception messages. Attached to those loggers (and to the
    root handlers), this keeps signatures out of the logs and out of Sentry breadcrumbs.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        scrubbed = scrub(message)
        if scrubbed != message:
            record.msg = scrubbed
            record.args = None
        if record.exc_info and not record.exc_text:
            record.exc_text = scrub(logging.Formatter().formatException(record.exc_info))
        return True


_LOGGERS_TO_SCRUB = (
    "urllib3.connectionpool",
    "urllib3.connection",
    "urllib3.response",
    "urllib3.poolmanager",
    "urllib3.util.retry",
    "requests",
    "prism.app",
    "prism.core",
    "prism.origins",
)


def install_log_scrubbing() -> None:
    """Attach ScrubSignedUrlsFilter to the loggers that may see signed URLs and to root handlers.

    A logger's filters only apply to records logged on that exact logger, so each emitting
    logger gets the filter; the root handlers get it too, for anything else that propagates.
    """
    scrubber = ScrubSignedUrlsFilter()
    targets: List[Any] = [logging.getLogger(name) for name in _LOGGERS_TO_SCRUB]
    targets.extend(logging.getLogger().handlers)
    for target in targets:
        if not any(isinstance(f, ScrubSignedUrlsFilter) for f in target.filters):
            target.addFilter(scrubber)


def _scrub_value(value: Any, depth: int = 0) -> Any:
    if depth > 30:
        return value
    if isinstance(value, str):
        return scrub(value)
    if isinstance(value, dict):
        return {key: _scrub_value(item, depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub_value(item, depth + 1) for item in value]
    if isinstance(value, tuple):
        return tuple(_scrub_value(item, depth + 1) for item in value)
    return value


def sentry_before_send(event: Dict[str, Any], hint: Dict[str, Any]) -> Dict[str, Any]:
    """Sentry hook that redacts signed-URL parameters anywhere in an event."""
    return _scrub_value(event)


def sentry_before_breadcrumb(crumb: Dict[str, Any], hint: Dict[str, Any]) -> Dict[str, Any]:
    """Sentry hook that redacts signed-URL parameters in breadcrumbs (HTTP calls, log lines)."""
    return _scrub_value(crumb)


# ---------------------------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------------------------


class OriginStats:
    """In-process counters of which origin served originals, how many bytes the fallback served,
    and what write-back did.

    Each uWSGI worker process keeps its own counters and logs them on ``prism.origins`` at INFO
    at most every ``interval`` seconds (checked when a counter changes), so the size of the
    remaining gap and the progress of write-back can be read from the logs.
    """

    def __init__(self, interval: float):
        self.interval = interval
        self._lock = threading.Lock()
        self._counts: collections.Counter = collections.Counter()
        self._last_logged = time.monotonic()

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[name] += amount
            now = time.monotonic()
            due = now - self._last_logged >= self.interval
            if due:
                self._last_logged = now
                snapshot = dict(self._counts)
        if due:
            self._log(snapshot)

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()
            self._last_logged = time.monotonic()

    def log_now(self) -> None:
        self._log(self.snapshot())

    @staticmethod
    def _log(counts: Dict[str, int]) -> None:
        summary = " ".join(f"{name}={counts[name]}" for name in sorted(counts))
        logger.info("origin stats pid=%d %s", os.getpid(), summary)


STATS = OriginStats(interval=settings.ORIGIN_STATS_INTERVAL)

# At most one Sentry event per (origin, kind) per this many seconds: an outage of the read
# bucket would otherwise send one event per request.
SENTRY_MIN_INTERVAL = 60.0
# At most one Sentry event per (bucket, kind of failure) per this many seconds for write-back,
# where one refused key usually means every copy is refused.
WRITE_BACK_SENTRY_MIN_INTERVAL = 300.0
_sentry_lock = threading.Lock()
_sentry_last_sent: Dict[str, float] = {}


def _capture(exc: BaseException, key: str, interval: float = SENTRY_MIN_INTERVAL) -> None:
    """Send ``exc`` to Sentry unless an event with the same ``key`` was sent within ``interval``.

    Without a configured DSN this does nothing, and an error inside the Sentry client is logged
    rather than raised, so reporting can never break a request or a write-back thread.
    """
    now = time.monotonic()
    with _sentry_lock:
        last = _sentry_last_sent.get(key)
        if last is not None and now - last < interval:
            return
        _sentry_last_sent[key] = now
    try:
        sentry_sdk.capture_exception(exc)
    except Exception:
        logger.exception("could not send an event to Sentry: key=%s", key)


# ---------------------------------------------------------------------------------------------
# Origins
# ---------------------------------------------------------------------------------------------


class S3Origin:
    """A bucket that originals are read from.

    Public buckets are read with a plain URL. Private buckets (``private: true`` in the customer
    credentials) are read with a short-lived signed URL made from the bucket's keys. The key of a
    private origin must be allowed to list the bucket: S3 and Ceph answer a missing key with 403
    instead of 404 to a caller that may not list, and a 403 from a private origin is treated as
    refused credentials.
    """

    def __init__(self, name, bucket_name, region, endpoint_url, key_id, secret_key, private):
        self.name = name
        self.bucket_name = bucket_name
        self.region = region
        self.endpoint_url = endpoint_url
        self.key_id = key_id
        self.secret_key = secret_key
        self.private = private

    def s3_config(self) -> core.S3ConnectionConfig:
        return core.S3ConnectionConfig(
            key_id=self.key_id,
            secret_key=self.secret_key,
            region=self.region,
            endpoint_url=self.endpoint_url,
        )

    def url(self, path: str, method: str = "GET", headers: Optional[Dict[str, str]] = None) -> str:
        if self.private:
            return core.get_signed_s3_url(self.bucket_name, path, self.s3_config(), method=method, headers=headers)
        return core.get_s3_url(self.bucket_name, self.region, path, endpoint=self.endpoint_url)

    def describe(self) -> str:
        return f"{self.name} bucket={self.bucket_name}"

    def __repr__(self) -> str:
        return f"S3Origin(name={self.name!r}, bucket_name={self.bucket_name!r}, private={self.private!r})"


class HttpOrigin:
    """An origin read with an anonymous GET of ``<base_url>/<key>``.

    Meant for a CDN such as a CloudFront distribution in front of the old bucket, which is cheaper
    to read from than the bucket itself. The key is percent-encoded, keeping ``/``.
    """

    private = False

    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.bucket_name = None
        # For logs: the base URL without any user:password@ part.
        self._display_url = re.sub(r"^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@]*@", r"\1", self.base_url)

    def url(self, path: str, method: str = "GET") -> str:
        return f"{self.base_url}/{urllib.parse.quote(path.lstrip('/'), safe='/')}"

    def describe(self) -> str:
        return f"{self.name} url={self._display_url}"

    def __repr__(self) -> str:
        return f"HttpOrigin(name={self.name!r}, base_url={self._display_url!r})"


# ---------------------------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------------------------


class OriginError(Exception):
    """An origin could not provide a usable original. ``kind`` says how the caller should react."""

    kind = "error"

    def __init__(self, origin, reason: str, etag: Optional[str] = None, status: Optional[int] = None):
        self.origin = origin
        self.reason = scrub(reason)
        # For a broken copy: the ETag of the copy that was read, so write-back replaces only it.
        self.etag = etag
        # The HTTP status the origin answered with, when the error came from one.
        self.status = status
        super().__init__(f"{origin.describe()}: {self.reason}")


class OriginMissing(OriginError):
    kind = "missing"


class OriginBroken(OriginError):
    kind = "broken"


class OriginUnavailable(OriginError):
    kind = "unavailable"


class OriginMisconfigured(OriginError):
    kind = "misconfigured"


class WriteBackFailed(Exception):
    """Sent to Sentry when the read bucket refused a write-back copy.

    That is a HEAD or PUT answered with an error, or a stored ETag that does not match the bytes
    sent. The message holds the bucket and the reason but not the key, so every failure of one
    kind is grouped into one Sentry issue.
    """


class ReadFailed(Exception):
    """No origin served the original. ``status`` is the HTTP status to answer with."""

    def __init__(self, status: int, reason: str):
        self.status = status
        self.reason = reason
        super().__init__(f"{status}: {reason}")


@dataclass
class Original:
    """An original image as read from an origin: its exact bytes and the decoded image."""

    data: bytes
    image: Any
    origin: Any
    content_type: Optional[str] = None
    etag: Optional[str] = None
    content_length: Optional[int] = None


_session_local = threading.local()


def _session() -> requests.Session:
    """A per-thread session with a small retry budget, so an unreachable origin fails fast.

    One retry on connection errors, timeouts and 500/502/503/504, then the last response (or
    error) is returned, with a ``(connect, read)`` timeout from settings. Sessions are per thread
    and per process because uWSGI forks workers after import.
    """
    session = getattr(_session_local, "session", None)
    if session is None or getattr(_session_local, "pid", None) != os.getpid():
        retry = Retry(
            total=settings.ORIGIN_RETRIES,
            backoff_factor=0.1,
            status_forcelist=(500, 502, 503, 504),
            raise_on_status=False,
            # A Retry-After header could otherwise stretch one retry into minutes.
            respect_retry_after_header=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _session_local.session = session
        _session_local.pid = os.getpid()
    return session


def _timeout() -> Tuple[float, float]:
    return (settings.ORIGIN_CONNECT_TIMEOUT, settings.ORIGIN_READ_TIMEOUT)


_S3_CODE_RE = re.compile(rb"<Code>([A-Za-z0-9.]+)</Code>")


def s3_error_code(response: requests.Response) -> Optional[str]:
    """The ``<Code>`` of an S3 error response body (NoSuchKey, NoSuchBucket, AccessDenied...)."""
    try:
        body = response.content[:4096] if response.content else b""
    except Exception:
        return None
    match = _S3_CODE_RE.search(body)
    return match.group(1).decode("ascii") if match else None


BODY_EXCERPT_CHARS = 200


def body_excerpt(response: requests.Response) -> str:
    """The start of a response body on one line, for logs.

    Whitespace is collapsed, signed-URL parameters are redacted, and the result is at most
    BODY_EXCERPT_CHARS characters long.
    """
    try:
        raw = response.content[:BODY_EXCERPT_CHARS * 4] if response.content else b""
    except Exception:
        return ""
    text = " ".join(raw.decode("utf-8", errors="replace").split())
    return scrub(text)[:BODY_EXCERPT_CHARS]


def classify_status(origin, status: int, code: Optional[str], excerpt: Optional[str] = None) -> OriginError:
    """Turn a non-2xx HTTP status from an origin into the matching OriginError.

    Redirects count as misconfiguration: S3 answers a request sent to the wrong regional
    endpoint with 301 PermanentRedirect. ``excerpt``, the start of the response body, is added
    to the reason after a colon, so the status stays the first part of it.
    """
    label = f"{status} {code}" if code else str(status)
    if excerpt:
        label = f"{label}: body {excerpt!r}"
    if status == 404:
        if code == "NoSuchBucket":
            return OriginMisconfigured(origin, label, status=status)
        return OriginMissing(origin, label, status=status)
    if status == 403:
        if origin.private:
            return OriginMisconfigured(origin, f"{label}: credentials refused (the key must be allowed to list the bucket)", status=status)
        return OriginMissing(origin, label, status=status)
    if status == 429 or status >= 500:
        return OriginUnavailable(origin, label, status=status)
    return OriginMisconfigured(origin, label, status=status)


def _status_error(origin, response: requests.Response, code: Optional[str]) -> OriginError:
    """classify_status for a response, with a body excerpt for an HTTP origin's error pages.

    A CDN answers a missing key with 404; for any other error status its body (an origin policy
    denial, a firewall block page, an error page) is what tells the operator what went wrong.
    """
    excerpt = None
    if isinstance(origin, HttpOrigin) and response.status_code != 404:
        excerpt = body_excerpt(response)
    return classify_status(origin, response.status_code, code, excerpt)


def _request(origin, method: str, url: str, **kwargs) -> requests.Response:
    """Send one request to an origin. Redirects are returned, not followed.

    A 3xx from an origin means the request went to the wrong place (S3 answers a request sent
    to the wrong regional endpoint with a redirect), and classify_status turns it into a 502
    without fallback. Following it would serve, or write to, whatever the redirect points at.
    """
    try:
        return _session().request(method, url, timeout=_timeout(), allow_redirects=False, **kwargs)
    except requests.RequestException as e:
        # The exception text holds the full URL; scrub() in OriginError removes signatures.
        raise OriginUnavailable(origin, f"{type(e).__name__}: {e}") from e


def fetch(origin, path: str) -> Original:
    """GET an original from one origin and decode it. Raises an OriginError subclass."""
    # identity: the bytes must be the stored object exactly, both to decode and to copy back.
    response = _request(origin, "GET", origin.url(path), headers={"Accept-Encoding": "identity"})
    if not 200 <= response.status_code < 300:
        raise _status_error(origin, response, s3_error_code(response))
    data = response.content
    etag = response.headers.get("ETag")
    declared = response.headers.get("Content-Length")
    content_length = int(declared) if declared and declared.isdigit() else None
    if not data:
        raise OriginBroken(origin, "empty", etag=etag)
    if content_length is not None and content_length != len(data):
        raise OriginBroken(origin, f"incomplete: {len(data)} of {content_length} bytes", etag=etag)
    try:
        image = Image(file=BytesIO(data))
    except Exception as e:
        raise OriginBroken(origin, f"not decodable ({type(e).__name__})", etag=etag)
    return Original(
        data=data,
        image=image,
        origin=origin,
        content_type=response.headers.get("Content-Type"),
        etag=etag,
        content_length=content_length,
    )


def probe(origin, path: str) -> None:
    """HEAD an original on one origin. Returns if it is there and non-empty, else raises.

    A HEAD response has no body, so a 404 does not say whether the key or the bucket is
    missing. Before a 404 counts as a miss, a one-byte GET reads the S3 error code, so that
    NoSuchBucket stops with 502 here as it does on the GET path. That GET costs one extra
    request per miss; a file that appeared in between answers it with 2xx and is used.
    """
    response = _request(origin, "HEAD", origin.url(path, method="HEAD"))
    if response.status_code == 404:
        response = _request(origin, "GET", origin.url(path), headers={"Range": "bytes=0-0"})
        if 200 <= response.status_code < 300:
            return
        raise _status_error(origin, response, s3_error_code(response))
    if not 200 <= response.status_code < 300:
        raise _status_error(origin, response, None)
    if response.headers.get("Content-Length") == "0":
        raise OriginBroken(origin, "empty", etag=response.headers.get("ETag"))


_PUBLIC_REASONS = {
    "empty": core.EmptyOriginalFile.message,
    "incomplete": "The original file is incomplete.",
    "not decodable": core.InvalidImageError.message,
}


def _public_broken_reason(error: OriginError) -> str:
    for prefix, message in _PUBLIC_REASONS.items():
        if error.reason.startswith(prefix):
            return message
    return "The original file is not usable."


def _try_origins(origins: List[Any], path: str, attempt: Callable[[Any, str], Any]):
    """Run ``attempt`` against each origin in turn; return ``(origin, result, failures)``.

    Raises ReadFailed when no origin succeeds: 502 if an origin was misconfigured or
    unavailable, 400 if an origin had the file but it was broken (as a single origin always
    answered), otherwise 404.
    """
    failures: List[OriginError] = []
    for origin in origins:
        try:
            result = attempt(origin, path)
        except OriginMisconfigured as e:
            logger.error("origin misconfigured, not falling back: origin=%s reason=%s path=%s", origin.describe(), e.reason, path)
            _capture(e, f"{origin.name}:misconfigured")
            STATS.incr(f"{origin.name}.misconfigured")
            raise ReadFailed(502, f"The {origin.name} origin is misconfigured ({e.reason.split(':')[0]}).")
        except OriginUnavailable as e:
            logger.warning("origin unavailable, trying the next one: origin=%s reason=%s path=%s", origin.describe(), e.reason, path)
            _capture(e, f"{origin.name}:unavailable")
            STATS.incr(f"{origin.name}.unavailable")
            failures.append(e)
        except (OriginMissing, OriginBroken) as e:
            # A miss on an HTTP origin other than a 404 (a 403 from a CDN) is still a miss, but
            # logged loudly: it usually means the CDN, not the file, is the problem.
            loud = isinstance(origin, HttpOrigin) and e.status not in (None, 404)
            level = logging.WARNING if loud else logging.INFO
            logger.log(level, "original %s on origin=%s reason=%s path=%s", e.kind, origin.describe(), e.reason, path)
            STATS.incr(f"{origin.name}.{e.kind}")
            failures.append(e)
        else:
            logger.info("original served by origin=%s path=%s", origin.describe(), path)
            STATS.incr(f"served.{origin.name}")
            return origin, result, failures

    unavailable = [f for f in failures if isinstance(f, OriginUnavailable)]
    broken = [f for f in failures if isinstance(f, OriginBroken)]
    if unavailable:
        names = ", ".join(f.origin.name for f in unavailable)
        raise ReadFailed(502, f"The original could not be read: the {names} origin is unavailable.")
    if broken:
        raise ReadFailed(400, _public_broken_reason(broken[0]))
    raise ReadFailed(404, "Not found.")


def read_original(path: str, origins: List[Any], write_back: Optional["WriteBackTarget"] = None,
                  write_back_queue: Optional["WriteBackQueue"] = None) -> Original:
    """Read an original from the first origin that has a usable copy.

    When ``write_back`` is given and the original came from a later origin while the first
    origin reported it missing or broken, the bytes are queued for copying to the first origin.
    Queueing never raises and never delays the response.
    """
    origin, original, failures = _try_origins(origins, path, fetch)
    if origin is not origins[0]:
        # Where the fallback is billed by transfer, bytes matter more than request counts.
        STATS.incr("fallback.bytes", len(original.data))
    if write_back is not None and origin is not origins[0] and failures:
        primary_failure = failures[0]
        if isinstance(primary_failure, (OriginMissing, OriginBroken)):
            try:
                (write_back_queue or default_write_back_queue()).submit(
                    WriteBackJob(
                        target=write_back,
                        key=path,
                        data=original.data,
                        content_type=original.content_type,
                        etag=original.etag,
                        content_length=original.content_length,
                        replace_broken=isinstance(primary_failure, OriginBroken),
                        broken_etag=primary_failure.etag,
                    )
                )
            except Exception:
                logger.exception("write-back could not be queued: path=%s", path)
        else:
            logger.info("write-back skipped: reason=%s origin is %s path=%s", primary_failure.origin.name, primary_failure.kind, path)
            STATS.incr("write_back.skipped")
    return original


def locate_original(path: str, origins: List[Any]):
    """Return the first origin holding a non-empty copy of ``path``, checked with HEAD.

    Uses the same rules as read_original, without downloading or decoding. For the GIF
    passthrough, which redirects the client to the original instead of processing it.
    """
    origin, _, _ = _try_origins(origins, path, probe)
    return origin


# ---------------------------------------------------------------------------------------------
# Write-back
# ---------------------------------------------------------------------------------------------


class WriteBackTarget(S3Origin):
    """The read bucket, addressed with its credentials so originals can be copied into it.

    Requests are always signed (SigV2 query signatures with path-style URLs on a custom
    endpoint, as for private reads), whether or not the bucket allows public reads.
    """

    def __init__(self, bucket_name, region, endpoint_url, key_id, secret_key):
        super().__init__("read", bucket_name, region, endpoint_url, key_id, secret_key, private=True)


@dataclass
class WriteBackJob:
    """One original to copy into the read bucket.

    It holds the fallback's bytes and response headers but not the decoded image, which can be
    tens of times larger and is not needed once the request has been answered.
    """

    target: WriteBackTarget
    key: str
    data: bytes
    content_type: Optional[str] = None
    # ETag and Content-Length of the fallback response, to check the bytes before copying them.
    etag: Optional[str] = None
    content_length: Optional[int] = None
    # True when the read bucket had an empty or undecodable copy that should be replaced.
    replace_broken: bool = False
    # ETag of that broken copy, so a copy that changed since it was read is left alone. When the
    # broken copy was read without one, it is never replaced.
    broken_etag: Optional[str] = None


_MD5_ETAG_RE = re.compile(r"^[0-9a-f]{32}$")


def _strip_etag(etag: Optional[str]) -> str:
    return (etag or "").strip().strip('"').lower()


def write_back_one(job: WriteBackJob) -> Tuple[str, str]:
    """Copy one original into the read bucket. Returns ``(outcome, detail)``.

    Outcomes: ``written`` (the key was missing), ``replaced-broken`` (a broken copy was
    overwritten), ``exists`` (a copy is already there and was left alone), ``skipped`` (the
    fallback bytes could not be verified, so nothing was written) and ``failed``.
    """
    data = job.data
    if not data:
        return "skipped", "empty"
    if job.content_length is None:
        return "skipped", "fallback response had no Content-Length"
    if job.content_length != len(data):
        return "skipped", f"length mismatch: {len(data)} of {job.content_length} bytes"
    digest = hashlib.md5(data)
    md5_hex = digest.hexdigest()
    fallback_etag = _strip_etag(job.etag)
    # A single-part S3 upload's ETag is the MD5 of its bytes; check it when it has that form.
    if _MD5_ETAG_RE.match(fallback_etag) and fallback_etag != md5_hex:
        return "skipped", "fallback ETag does not match the bytes received"

    target = job.target
    head = _request(target, "HEAD", target.url(job.key, method="HEAD"))
    if head.status_code == 200:
        existing_etag = _strip_etag(head.headers.get("ETag"))
        if existing_etag == md5_hex:
            return "exists", "identical copy already present"
        if not job.replace_broken:
            return "exists", "a copy appeared since the read; left alone"
        if not job.broken_etag:
            # Without it, a good copy written since the read would look the same as the broken one.
            return "exists", "the broken copy was read without an ETag; left alone"
        if existing_etag != _strip_etag(job.broken_etag):
            return "exists", "the broken copy changed since the read; left alone"
        outcome = "replaced-broken"
    elif head.status_code == 404:
        outcome = "written"
    else:
        return "failed", f"HEAD {classify_status(target, head.status_code, None).reason}"

    headers = {
        "Content-Type": job.content_type or "application/octet-stream",
        "Content-MD5": base64.b64encode(digest.digest()).decode("ascii"),
    }
    put = _request(target, "PUT", target.url(job.key, method="PUT", headers=headers), data=data, headers=headers)
    if put.status_code >= 300:
        return "failed", f"PUT {put.status_code} {s3_error_code(put) or ''}".strip()
    stored_etag = _strip_etag(put.headers.get("ETag"))
    if stored_etag and _MD5_ETAG_RE.match(stored_etag) and stored_etag != md5_hex:
        return "failed", "stored ETag does not match the bytes sent"
    return outcome, f"{len(data)} bytes"


class WriteBackQueue:
    """A bounded queue and a few background threads that run write_back_one.

    ``submit`` never blocks: when the queue is full, or the bytes waiting would exceed
    ``max_pending_bytes``, the job is dropped and logged; the next request for that original
    reads the fallback again and queues it again. A key that is already waiting is not queued
    twice. Threads start on first use in each process (uWSGI forks workers after import).
    Jobs still waiting when a worker process exits are lost, which only costs a later re-read.
    """

    def __init__(self, workers: int, max_items: int, max_pending_bytes: int,
                 stats: Optional[OriginStats] = None, autostart: bool = True):
        self.workers = workers
        self.max_items = max_items
        self.max_pending_bytes = max_pending_bytes
        self.stats = stats or STATS
        self.autostart = autostart
        self._lock = threading.Lock()
        self._pid: Optional[int] = None
        self._queue: "queue.Queue[WriteBackJob]" = queue.Queue(maxsize=max_items)
        self._pending_keys: set = set()
        self._pending_bytes = 0

    def _ensure_started(self) -> None:
        if self._pid == os.getpid():
            return
        self._queue = queue.Queue(maxsize=self.max_items)
        self._pending_keys = set()
        self._pending_bytes = 0
        self._pid = os.getpid()
        if self.autostart:
            for i in range(self.workers):
                threading.Thread(target=self._run, name=f"prism-write-back-{i}", daemon=True).start()

    def submit(self, job: WriteBackJob) -> bool:
        size = len(job.data)
        with self._lock:
            self._ensure_started()
            if job.key in self._pending_keys:
                self.stats.incr("write_back.already_queued")
                return False
            if self._pending_bytes + size > self.max_pending_bytes:
                reason = "pending bytes limit"
            else:
                try:
                    self._queue.put_nowait(job)
                except queue.Full:
                    reason = "queue full"
                else:
                    self._pending_keys.add(job.key)
                    self._pending_bytes += size
                    self.stats.incr("write_back.queued")
                    return True
        logger.warning("write-back dropped: reason=%s path=%s", reason, job.key)
        self.stats.incr("write_back.dropped")
        return False

    def run_pending(self) -> None:
        """Process every waiting job on the calling thread. For tests and autostart=False."""
        while True:
            try:
                job = self._queue.get_nowait()
            except queue.Empty:
                return
            self._process(job)

    def _run(self) -> None:
        while True:
            job = self._queue.get()
            self._process(job)

    def _process(self, job: WriteBackJob) -> None:
        # A failed copy is re-read from the fallback on the next request, so without a Sentry
        # event a read bucket that refuses writes shows up only as more fallback traffic.
        # ``kind`` groups the failures for throttling: the HEAD/PUT status, "unavailable", or
        # the exception type.
        error: Optional[BaseException] = None
        kind = None
        try:
            outcome, detail = write_back_one(job)
            kind = detail
        except OriginUnavailable as e:
            outcome, detail, error, kind = "failed", e.reason, e, "unavailable"
        except Exception as e:
            outcome, detail, error, kind = "failed", scrub(f"{type(e).__name__}: {e}"), e, type(e).__name__
        finally:
            with self._lock:
                self._pending_keys.discard(job.key)
                self._pending_bytes -= len(job.data)
            self._queue.task_done()
        level = logging.WARNING if outcome == "failed" else logging.INFO
        logger.log(level, "write-back %s: %s bucket=%s path=%s", outcome, detail, job.target.bucket_name, job.key)
        self.stats.incr(f"write_back.{outcome}")
        if outcome == "failed":
            if error is None:
                error = WriteBackFailed(f"write-back failed: {detail} bucket={job.target.bucket_name}")
            _capture(error, f"write_back:{job.target.bucket_name}:{kind}", interval=WRITE_BACK_SENTRY_MIN_INTERVAL)

    def join(self) -> None:
        self._queue.join()


_default_queue: Optional[WriteBackQueue] = None
_default_queue_lock = threading.Lock()


def default_write_back_queue() -> WriteBackQueue:
    global _default_queue
    with _default_queue_lock:
        if _default_queue is None:
            _default_queue = WriteBackQueue(
                workers=settings.WRITE_BACK_WORKERS,
                max_items=settings.WRITE_BACK_QUEUE_SIZE,
                max_pending_bytes=settings.WRITE_BACK_MAX_PENDING_MB * 1024 * 1024,
            )
        return _default_queue
