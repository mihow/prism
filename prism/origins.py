"""Read original images from an ordered list of origins, and copy fallback reads back.

A customer normally has one origin: its read bucket. During a storage migration it can also
have a fallback origin (a second bucket) that holds the
originals which have not been copied to the read bucket yet. This module decides, for each
original, which origin serves it:

* **Missing** on an origin (404, or 403 from a public origin, which is how S3 answers anonymous
  callers for a missing key) or **broken** there (empty, shorter than its Content-Length, or not
  decodable as an image): try the next origin quietly, logging the reason at INFO.
* **Unavailable** (connection error, timeout, 429 or 5xx): try the next origin, but loudly, with
  a WARNING and a Sentry event, because every such request costs a fallback read.
* **Misconfigured** (NoSuchBucket, a 403 from a private origin, a redirect, or any other 4xx): stop and
  return 502 without falling back, logged at ERROR and sent to Sentry. A wrong bucket name or
  key must not quietly move all traffic to the fallback.

Decoding catches empty and unreadable files but not every damaged one: ImageMagick decodes a
JPEG that is cut short (the missing rows come out grey), so a truncated copy on the read
bucket is served as it is. Nothing here verifies checksums on the read path.

All origin logging goes to the ``prism.origins`` logger, which can be enabled on its own with
the ``ORIGINS_LOG_LEVEL`` setting.
"""
import collections
import logging
import os
import re
import threading
import time
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
    """In-process counters of which origin served originals and what write-back did.

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
_sentry_lock = threading.Lock()
_sentry_last_sent: Dict[str, float] = {}


def _capture(exc: BaseException, key: str) -> None:
    now = time.monotonic()
    with _sentry_lock:
        last = _sentry_last_sent.get(key)
        if last is not None and now - last < SENTRY_MIN_INTERVAL:
            return
        _sentry_last_sent[key] = now
    sentry_sdk.capture_exception(exc)


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

    def url(self, path: str, method: str = "GET") -> str:
        if self.private:
            return core.get_signed_s3_url(self.bucket_name, path, self.s3_config(), method=method)
        return core.get_s3_url(self.bucket_name, self.region, path, endpoint=self.endpoint_url)

    def describe(self) -> str:
        return f"{self.name} bucket={self.bucket_name}"

    def __repr__(self) -> str:
        return f"S3Origin(name={self.name!r}, bucket_name={self.bucket_name!r}, private={self.private!r})"


# ---------------------------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------------------------


class OriginError(Exception):
    """An origin could not provide a usable original. ``kind`` says how the caller should react."""

    kind = "error"

    def __init__(self, origin, reason: str, etag: Optional[str] = None):
        self.origin = origin
        self.reason = scrub(reason)
        # For a broken copy: the ETag of the copy that was read, so write-back replaces only it.
        self.etag = etag
        super().__init__(f"{origin.describe()}: {self.reason}")


class OriginMissing(OriginError):
    kind = "missing"


class OriginBroken(OriginError):
    kind = "broken"


class OriginUnavailable(OriginError):
    kind = "unavailable"


class OriginMisconfigured(OriginError):
    kind = "misconfigured"


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


def classify_status(origin, status: int, code: Optional[str]) -> OriginError:
    """Turn a non-2xx HTTP status from an origin into the matching OriginError.

    Redirects count as misconfiguration: S3 answers a request sent to the wrong regional
    endpoint with 301 PermanentRedirect.
    """
    label = f"{status} {code}" if code else str(status)
    if status == 404:
        if code == "NoSuchBucket":
            return OriginMisconfigured(origin, label)
        return OriginMissing(origin, label)
    if status == 403:
        if origin.private:
            return OriginMisconfigured(origin, f"{label}: credentials refused (the key must be allowed to list the bucket)")
        return OriginMissing(origin, label)
    if status == 429 or status >= 500:
        return OriginUnavailable(origin, label)
    return OriginMisconfigured(origin, label)


def _request(origin, method: str, url: str, **kwargs) -> requests.Response:
    try:
        return _session().request(method, url, timeout=_timeout(), **kwargs)
    except requests.RequestException as e:
        # The exception text holds the full URL; scrub() in OriginError removes signatures.
        raise OriginUnavailable(origin, f"{type(e).__name__}: {e}") from e


def fetch(origin, path: str) -> Original:
    """GET an original from one origin and decode it. Raises an OriginError subclass."""
    # identity: the bytes must be the stored object exactly, both to decode and to copy back.
    response = _request(origin, "GET", origin.url(path), headers={"Accept-Encoding": "identity"})
    if not 200 <= response.status_code < 300:
        raise classify_status(origin, response.status_code, s3_error_code(response))
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
    """HEAD an original on one origin. Returns if it is there and non-empty, else raises."""
    response = _request(origin, "HEAD", origin.url(path, method="HEAD"))
    if not 200 <= response.status_code < 300:
        raise classify_status(origin, response.status_code, None)
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
            logger.info("original %s on origin=%s reason=%s path=%s", e.kind, origin.describe(), e.reason, path)
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


def read_original(path: str, origins: List[Any]) -> Original:
    """Read an original from the first origin that has a usable copy."""
    _, original, _ = _try_origins(origins, path, fetch)
    return original


def locate_original(path: str, origins: List[Any]):
    """Return the first origin holding a non-empty copy of ``path``, checked with HEAD.

    Uses the same rules as read_original, without downloading or decoding. For the GIF
    passthrough, which redirects the client to the original instead of processing it.
    """
    origin, _, _ = _try_origins(origins, path, probe)
    return origin
