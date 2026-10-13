import os


DOMAIN = os.environ.get('DOMAIN')
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_WRITE_BUCKET = os.environ.get('S3_WRITE_BUCKET')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')
AWS_REGION = os.environ.get('AWS_REGION')
TEST_IMAGE = os.environ.get('TEST_IMAGE')
MULTI_CUSTOMER_MODE = os.environ.get('MULTI_CUSTOMER_MODE', 'false').lower() == 'true'
SECRETS_BUCKET = os.environ.get('SECRETS_BUCKET')
DEFAULT_CUSTOMER = os.environ.get('DEFAULT_CUSTOMER')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Reading originals (see prism/origins.py). Each origin request gets at most ORIGIN_RETRIES
# retries, so an unreachable origin costs a few seconds per request rather than half a minute.
ORIGIN_CONNECT_TIMEOUT = float(os.environ.get('ORIGIN_CONNECT_TIMEOUT', '3'))
ORIGIN_READ_TIMEOUT = float(os.environ.get('ORIGIN_READ_TIMEOUT', '5'))
ORIGIN_RETRIES = int(os.environ.get('ORIGIN_RETRIES', '1'))
# Level for the "prism.origins" logger alone (for example INFO), independent of LOG_LEVEL.
ORIGINS_LOG_LEVEL = os.environ.get('ORIGINS_LOG_LEVEL')
# How often each worker process logs its origin and write-back counters, in seconds.
ORIGIN_STATS_INTERVAL = float(os.environ.get('ORIGIN_STATS_INTERVAL', '300'))
# Background copying of originals from the fallback origin back to the read bucket.
WRITE_BACK_WORKERS = int(os.environ.get('WRITE_BACK_WORKERS', '2'))
WRITE_BACK_QUEUE_SIZE = int(os.environ.get('WRITE_BACK_QUEUE_SIZE', '64'))
WRITE_BACK_MAX_PENDING_MB = int(os.environ.get('WRITE_BACK_MAX_PENDING_MB', '256'))

# Degraded mode for when the write bucket cannot be written to (for example, an object store
# whose authentication service is down while anonymous reads still work). With LOCAL_CACHE_DIR
# set, a newly made variant that cannot be stored remotely is kept on local disk and served from
# there, instead of the request failing. SKIP_REMOTE_WRITE stops even trying the remote upload,
# so new variants are answered as soon as they are made. Both default to off.
LOCAL_CACHE_DIR = os.environ.get('PRISM_LOCAL_CACHE_DIR') or None
SKIP_REMOTE_WRITE = os.environ.get('PRISM_SKIP_REMOTE_WRITE', 'false').lower() == 'true'
# Optional fallback for originals in single-customer mode (the multi-customer mode reads it
# from each customer's entry instead).
FALLBACK_CDN_URL = os.environ.get('FALLBACK_CDN_URL') or None
