# Prism - the image transformation service from Hipo


[![Docker Image Version](https://img.shields.io/docker/v/hipolabs/prism?label=hipolabs%2Fprism)](https://hub.docker.com/r/hipolabs/prism 'DockerHub')

## How It Works
Prism is an image transformation proxy for AWS S3. The source image is determined from the URL path. The transformation is determined from the URL query parameters. The transformed image is uploaded to S3 and a HTTP 302 Redirect response is returned pointing to the new image. Subsequent requests for the same image with the same parameters return the same S3 redirect without reprocessing the image.

![Prism Flow Diagram](flow.png)

### Example request:
http://prism-dev.tryprism.com -> This is the live server domain.

URL: `https://prism-dev.tryprism.com/images/test-1.jpg?h=200`  
Image file: `images/test-1.jpg`  
Parameters: `h=200`

Response Redirect Location: `https://s3.amazonaws.com/tryprism-dev/prism-images/images/test-1.jpg--resize--h__200.jpg`


### Usage

#### Set width
`http://prism-dev.tryprism.com/images/test-1.jpg?w=100`  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?w=100)

#### Set height 
`http://prism-dev.tryprism.com/images/test-1.jpg?h=100`  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?h=100)

#### Set output format 
`http://prism-dev.tryprism.com/images/test-1.jpg?h=100&out=png`  
out options are `jpg`, `png`, `webp`.  
If no option is specified and the client accepts webp then webp will be used by default)  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?h=100&out=png)

#### Pad image to fit the exact dimensions specified
`http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize_then_fit&w=100&h=100`  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize_then_fit&w=100&h=100)

#### Pad image to fit the exact dimensions specified, with background color specified
`http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize_then_fit&w=100&h=100&frame_bg_color=000`  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize_then_fit&w=100&h=100&frame_bg_color=000)

#### Resize and crop to dimensions
`http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize_then_crop&w=100&h=100`  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize_then_crop&w=100&h=100)

#### Crop first and resize to dimensions
`http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize&w=100&h=100&crop_x=0&crop_y=0&crop_width=200&crop_height=200`  
(crop_* parameters are relative to original image dimensions)  
![ ](http://prism-dev.tryprism.com/images/test-1.jpg?cmd=resize&w=100&h=100&crop_x=0&crop_y=0&crop_width=200&crop_height=200)





## Configuration
Prism has two different modes of operation; Single Customer Mode and Multi Customer Mode.

### Single Customer Mode (default)
In Single Customer Mode Prism reads and writes to a single S3 bucket. Use this mode when serving images for a single project.

Required Environment Variables:
* DOMAIN [`images.myproject.com`]
* S3_BUCKET [`myproject-images`]
* TEST_IMAGE [`images/test-1.jpg`]
* AWS_REGION [`us-east-1`]
* AWS_ACCESS_KEY_ID (The access key must provide read & write access to the `S3_BUCKET` (and `S3_WRITE_BUCKET` if provided).
* AWS_SECRET_ACCESS_KEY

Optional Environment Variables:
* S3_WRITE_BUCKET [`myproject-prism-images`] (If not provided, default is `S3_BUCKET`.)
* S3_ENDPOINT_URL (If using a non-AWS implementation of S3 like Ceph, MinIO, DigitalOcean Spaces, etc)

### Multi Customer Mode
In Multi Customer Mode Prism can handle requests for multiple customers together, where each customer has a separate S3 bucket and separate credentials. The customers are separated by subdomain. The configuration and credentials for each subdomain are loaded from a `credentials.json` stored in the SECRETS_BUCKET. 

Required Environment Variables:
* MULTI_CUSTOMER_MODE=true
* DOMAIN [`tryprism.com`]
* SECRETS_BUCKET [`super-secret-private-bucket`]
* DEFAULT_CUSTOMER (for tests) [`prism-test`]
* TEST_IMAGE [`images/test-1.jpg`]
* AWS_REGION (for the SECRETS_BUCKET) [`us-east-1`]
* AWS_ACCESS_KEY_ID (for the SECRETS_BUCKET)
* AWS_SECRET_ACCESS_KEY (for the SECRETS_BUCKET)


#### Example credentials.json
This JSON file maps subdomains to customer credentials.

WARNING: This file must not be publicly accessible!

```
{
    "foo": {
        "read_bucket_name": "foo-production",
        "read_bucket_region": "us-east-1",
        "read_bucket_key_id": "AKIABLABLA...",
        "read_bucket_secret_key": "XFiefjlfkjgls....",
    },
    "bar": {
        "read_bucket_name": "bar-images",
        "read_bucket_region": "eu-west-1",
        "read_bucket_key_id": "AKIAKABC...",
        "read_bucket_secret_key": "SNf1jJf2ffD....",
    },
}
```

Note: `write_bucket_*` parameters may be included to separate read and write buckets.

#### Private read buckets and a fallback origin
Prism fetches originals with a plain GET, so by default the read bucket must allow public reads.
Set `"read_bucket_private": true` to fetch originals with a short-lived signed URL made from the
`read_bucket_key_id` and `read_bucket_secret_key` instead (both are then required). That key must be
allowed to list the bucket: without list permission, S3-compatible stores answer a request for a
missing key with 403 instead of 404, and Prism treats a 403 from a private bucket as refused
credentials.

A customer may also name a fallback origin that holds originals the read bucket does not have yet,
for example while originals are migrated from one object store to another. The fallback is either a
second bucket (`fallback_bucket_name`, plus the optional `fallback_bucket_region`,
`fallback_bucket_endpoint_url`, `fallback_bucket_key_id`, `fallback_bucket_secret_key` and
`fallback_bucket_private`) or an HTTP(S) base URL, such as a CDN distribution in front of the old
bucket (`fallback_cdn_url`; Prism requests `<fallback_cdn_url>/<key>` anonymously). Set one or the
other, not both.

What Prism does depends on how the read bucket answers:

| Read bucket answer | What Prism does |
|---|---|
| The original | Serves it; the fallback is not contacted |
| 404 `NoSuchKey`, or 403 from a public bucket | Tries the fallback (logged at `INFO`) |
| An original that is empty, shorter than its `Content-Length`, or not decodable as an image | Tries the fallback (logged at `INFO`) |
| Connection error, timeout, 429 or 5xx, after one retry | Tries the fallback, loudly: a `WARNING` and a Sentry event (at most one event per bucket or CDN per minute and worker process; each customer's buckets are counted separately) |
| 404 `NoSuchBucket`, 403 from a private bucket, a redirect, or any other 4xx | Answers 502 without trying the fallback, logged at `ERROR` and sent to Sentry, so a misconfigured read bucket does not quietly send every request to the fallback |

A CDN fallback (`fallback_cdn_url`) follows the same rules, except that it answers a missing key
with 404, so any other error status from it is logged at `WARNING` with the status and the first
200 characters of the response body. A 403 from the CDN, which usually means an origin policy, a
firewall rule or an error page rather than a missing file, is still answered with 404 to the client.

When no origin can serve the original, Prism answers 404 if it is missing everywhere, 400 if a copy
exists but is empty or not decodable (with the same messages as before), and 502 if an origin could
not be read. Decoding does not catch every damaged file: ImageMagick decodes a JPEG that is cut
short, so a truncated copy in the read bucket is served as it is.

The GIF passthrough (`.gif` requested without `out=`) follows the same rules with a HEAD request
and redirects to the origin that has the file. A HEAD response has no body, so after a 404 Prism
sends a one-byte GET to read the S3 error code; a missing bucket then answers 502 here too. A redirect to a private bucket carries a signed URL
and is sent with `Cache-Control: no-store`.

#### Copying fallback reads into the read bucket
With `"fallback_write_back": true`, an original that was served by the fallback is copied into the
read bucket under the same key, so the next request for it does not reach the fallback again. The
copy uses the read bucket's key and secret (so both are required), and it happens in background
threads after the response is sent: a slow or failing write never delays or fails a request.

- Only the original bytes are copied, never a resized image, and only when they decoded as an
  image, their length matches the fallback's `Content-Length`, and their MD5 matches the fallback's
  `ETag` when that ETag is a plain MD5 (single-part uploads).
- A copy already in the read bucket is left alone, unless it is the empty or undecodable copy that
  was just read there, which is replaced. The check is a HEAD before the PUT; the PUT carries
  `Content-MD5` and the fallback's `Content-Type`.
- Nothing is copied when the read bucket was unreachable rather than missing the file.
- The queue is bounded (`WRITE_BACK_QUEUE_SIZE` jobs and `WRITE_BACK_MAX_PENDING_MB` of bytes per
  worker process). When it is full the copy is dropped and logged; the next request for that
  original reads the fallback and queues it again. Jobs still queued when a worker process exits
  are lost the same way.
- One queue per worker process serves every customer. A key already waiting for the same read
  bucket is not queued twice; the same key for another customer's read bucket is its own copy.

Each copy is logged on `prism.origins` with its outcome: `written`, `replaced-broken`, `exists`,
`skipped` (the bytes could not be verified), `failed` (with the reason) or `dropped`. A `failed`
copy is also sent to Sentry, at most once per read bucket (told apart by endpoint, name and key, so
customers never share a limit) and kind of failure (for example
`PUT 403 AccessDenied`) every five minutes per worker process, because a read bucket that refuses
writes otherwise shows up only as continued fallback traffic.

#### Origin logging and settings
Everything about origins is logged on the `prism.origins` logger. Set `ORIGINS_LOG_LEVEL=INFO` to see
which origin served each original, and each copy, without raising `LOG_LEVEL` for everything else.
Each worker process also logs its counters (originals served per origin, misses, bytes read from
the fallback, write-back outcomes) at most every `ORIGIN_STATS_INTERVAL` seconds, for example
`origin stats pid=12 fallback.bytes=36864000 read.missing=40 served.fallback=40 served.read=960 write_back.written=38 ...`.
`fallback.bytes` is the total size of the originals the fallback served, for a fallback billed by
transfer; it does not include failed or undecodable reads.
Signed-URL signatures and access key ids are redacted from these logs, from urllib3's retry
warnings and from Sentry events.

| Environment variable | Default | Meaning |
|---|---|---|
| `ORIGIN_CONNECT_TIMEOUT` | `3` | Seconds to wait for a connection to an origin |
| `ORIGIN_READ_TIMEOUT` | `5` | Seconds to wait between bytes from an origin |
| `ORIGIN_RETRIES` | `1` | Retries per origin request on connection errors, timeouts and 500/502/503/504 |
| `ORIGINS_LOG_LEVEL` | unset | Level for the `prism.origins` logger alone |
| `ORIGIN_STATS_INTERVAL` | `300` | Seconds between counter log lines per worker process |
| `WRITE_BACK_WORKERS` | `2` | Background copy threads per worker process |
| `WRITE_BACK_QUEUE_SIZE` | `64` | Copies waiting per worker process |
| `WRITE_BACK_MAX_PENDING_MB` | `256` | Bytes waiting per worker process |

Existing customers see two differences: an origin that answers 5xx or cannot be reached now gives
502 instead of an unhandled 500, and origin requests are retried once instead of five times.

```
{
    "foo": {
        "read_bucket_name": "foo-originals",
        "read_bucket_endpoint_url": "https://ceph.example.org",
        "read_bucket_region": "N/A",
        "read_bucket_key_id": "...",
        "read_bucket_secret_key": "...",
        "read_bucket_private": true,
        "write_bucket_name": "foo-thumbnails",
        "fallback_cdn_url": "https://d111111abcdef8.cloudfront.net",
        "fallback_write_back": true
    }
}
```

### TEST_IMAGE
The TEST_IMAGE setting is used to provide an image to be used for the test and health check endpoints. In multi customer mode the DEFAULT_CUSTOMER setting must also be set for the test endpoints to work.

The health check (`/elb-health/`) answers 200 when the default customer's TEST_IMAGE can be served
under the origin rules above, checked with HEAD requests: a test image that only the fallback holds,
or a read bucket that is down while the fallback works, still counts as healthy (the outage is
logged and sent to Sentry as for any request), while a misconfigured read bucket answers 500. Other
customers are not checked, so one customer's broken entry cannot take every instance out of the
load balancer. Each check counts as a served original in the origin counters.

### uWSGI Configuration

The Prism app runs under uWSGI. By default, it runs with 2 processes and 2 threads per process. These settings can be overridden using the UWSGI_PROCESSES and UWSGI_THREADS environment variables. Similarly, other options can be passed to uWSGI using UWSGI_* environment variables.


## Deployment
The Docker container runs a uwsgi process with a HTTP socket (port 8000) and a uwsgi socket (port 3001). For local development and testing connecting to the HTTP server is sufficient. For production use it is recommended to use Nginx in front of uwsgi. A sample Nginx configuration including caching setup is included here: [nginx-sample.conf](nginx-sample.conf)

To run docker container use following command:

`docker-compose -f docker-compose.yml -f docker-compose.development.yml up`

The `8000` port of the container is mapped to the `8001` port of the host. Use `localhost:8001` to access the app.

`http://localhost:8001/test`. This test url runs the same command both on your local and the live Prism server, and provides comparisons between local and live prism server image resizing operations. 

