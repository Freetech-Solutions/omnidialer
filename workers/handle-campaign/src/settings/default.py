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

# Predictive pacing (global defaults; overridable by env — not per-campaign yet)
# D_max: abandon rate ceiling (EXIT_ABANDON+EXIT_TIMEOUT) / connects humanos
MAX_ABANDON_RATE = float(os.getenv('DIALER_MAX_ABANDON_RATE', '0.03'))
# Progressive strict until ATT_COUNT reaches this sample size
WARM_UP_SAMPLE_SIZE = int(os.getenv('DIALER_WARM_UP_SAMPLE_SIZE', '50'))
# Target cadence for predictive evaluation ticks (milliseconds)
PREDICTIVE_TICK_MS = int(os.getenv('DIALER_PREDICTIVE_TICK_MS', '1000'))
# Floor for P_hit used by pacing helpers (anti-división / anti-explosión)
HIT_RATE_FLOOR = float(os.getenv('DIALER_HIT_RATE_FLOOR', '0.05'))
# EWMA alpha for DROP_RATE_EWMA (symmetric: toward 1 on abandon, 0 on hit).
# Source of γ; effective window ≈ 2/α − 1 connects. Cumulative DROP_RATE is reporting only.
DROP_RATE_ALPHA = float(os.getenv('DIALER_DROP_RATE_ALPHA', '0.1'))
# Fallback ART (seconds) when CAMP:{id}:ART aún no tiene muestra
DEFAULT_ART_SEC = float(os.getenv('DIALER_DEFAULT_ART_SEC', '15'))
# H7: fallback TOTAL_ANALYSIS_TIME (s) si campaña AMD sin muestra medida
DEFAULT_AMD_FALLBACK_SEC = float(os.getenv('DIALER_DEFAULT_AMD_FALLBACK_SEC', '5.0'))
# TTL cache lectura OML:AMD_CONF / OML:CAMP AMD
AMD_CONF_CACHE_TTL_SEC = int(os.getenv('DIALER_AMD_CONF_CACHE_TTL_SEC', '60'))
# Floor for expected remaining busy time in P_lib (seconds)
P_LIB_REMAINING_EPS = float(os.getenv('DIALER_P_LIB_REMAINING_EPS', '1.0'))
# Global kill-switch for predictive C_dial (campaign flag still required)
_raw_pred_enabled = os.getenv('DIALER_PREDICTIVE_ENABLED', 'true').strip().lower()
PREDICTIVE_ENABLED = _raw_pred_enabled in ('1', 'true', 'yes', 'on')
# Gamma floor when drop is between 0.5*D_max and D_max (before forcing progressive)
GAMMA_THROTTLE_FLOOR = float(os.getenv('DIALER_GAMMA_THROTTLE_FLOOR', '0.2'))
# TTL for CAMP:{id}:PACING snapshot (observability of last predictive tick)
PACING_SNAPSHOT_TTL_SEC = int(os.getenv('DIALER_PACING_SNAPSHOT_TTL_SEC', '30'))
# Kill-switch: consecutive ticks with D >= D_max before latching progressive R=1
THROTTLE_STREAK_K = int(os.getenv('DIALER_THROTTLE_STREAK_K', '5'))
# Exit latched throttle only when D < THROTTLE_EXIT_RATIO * D_max (hysteresis)
THROTTLE_EXIT_RATIO = float(os.getenv('DIALER_THROTTLE_EXIT_RATIO', '0.8'))
