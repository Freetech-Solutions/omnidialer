import logging
import os

logger = logging.getLogger(__name__)

_gearman_job_servers = os.getenv('GEARMAN_JOB_SERVERS', '')
GEARMAN_JOB_SERVERS = _gearman_job_servers.split('|') if _gearman_job_servers else []
if not GEARMAN_JOB_SERVERS:
    logger.warning('GEARMAN_JOB_SERVERS is not configured; no Gearman job servers available.')

REDIS_DIALER_SERVER = os.getenv('REDIS_DIALER_SERVER', 'omnidialer-redis')

REDIS_DIALER_PORT = os.getenv('REDIS_DIALER_PORT', 6380)

_gearman_jobs = os.getenv('GEARMAN_JOBS', '')
GEARMAN_JOBS = _gearman_jobs.split('|') if _gearman_jobs else []
if not GEARMAN_JOBS:
    logger.warning('GEARMAN_JOBS is not configured; no Gearman jobs available.')

TIME_BETWEEN_CALLS = os.getenv('TIME_BETWEEN_CALLS')

CHANNEL_AUDIT_INTERVAL_SEC = int(os.getenv('DIALER_CHANNEL_AUDIT_INTERVAL_SEC', '60'))
CHANNEL_AUDIT_LOCK_TTL_SEC = int(os.getenv('DIALER_CHANNEL_AUDIT_LOCK_TTL_SEC', '55'))
RESERVE_GRACE_SEC = int(os.getenv('DIALER_RESERVE_GRACE_SEC', '30'))
