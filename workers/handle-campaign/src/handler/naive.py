# -*- coding: utf-8 -*-

from .basic import DialerWorker

import atexit
import signal
import threading

from .utils import timed_lru_cache

import re
import json
import os
import socket
import uuid
import redis
from psycopg_pool import ConnectionPool
import gearman
import datetime
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.redis import RedisJobStore

from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_REMOVED,
    EVENT_JOB_MISSED,
)

from datetime import timedelta
from math import floor, ceil, exp
from time import sleep

from settings.default import (
    REDIS_DIALER_PORT,
    REDIS_DIALER_SERVER,
    GEARMAN_JOB_SERVERS,
    TIME_BETWEEN_CALLS,
    CHANNEL_AUDIT_INTERVAL_SEC,
    CHANNEL_AUDIT_LOCK_TTL_SEC,
    RESERVE_GRACE_SEC,
    AUDIT_UP_STREAK_TICKS,
    AUDIT_DOWN_STREAK_TICKS,
    AUDIT_IDLE_DOWN_STREAK_TICKS,
    ACD_STALE_EVICT_TICKS,
    AUDIT_INCOMPLETE_DOWN_STREAK_TICKS,
    MAX_ABANDON_RATE,
    WARM_UP_SAMPLE_SIZE,
    PREDICTIVE_TICK_MS,
    HIT_RATE_FLOOR,
    DROP_RATE_ALPHA,
    DEFAULT_ART_SEC,
    DEFAULT_AMD_FALLBACK_SEC,
    AMD_CONF_CACHE_TTL_SEC,
    P_LIB_REMAINING_EPS,
    PREDICTIVE_ENABLED,
    GAMMA_THROTTLE_FLOOR,
    MAX_DIAL_RATIO,
    PACING_SNAPSHOT_TTL_SEC,
    THROTTLE_STREAK_K,
    THROTTLE_STREAK_SEC,
    THROTTLE_MIN_HITS,
    THROTTLE_EXIT_RATIO,
    THROTTLE_STATE_TTL_SEC,
    CAPS_REVALIDATE_EVERY,
    REQUIRE_ENDPOINTS,
    ATTEMPT_ID_ENABLED,
)
from handler.dial_attempt import (
    get_attempt_id,
    new_attempt_id,
    store_attempt_id,
)
from handler.event_stream import notify_dialer_event
from handler.origin_guardrails import (
    check_endpoints_or_raise,
    evaluate_backpressure_guardrails,
    try_consume_global_caps,
)
from handler.predictive_pacer import (
    apply_channel_caps,
    decide_predictive_pace,
)

import logging

from ui.rendering import AdminRender
# --- logging primero (ANTES de atexit/signal) ---
LOGLEVEL = os.environ.get('PYTHON_LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOGLEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flag global para registrar listener una sola vez
_SCHED_LISTENER_REGISTERED = False
_AUDIT_JOB_REGISTERED = False
# Flag global para shutdown ordenado
_SCHED_SHUTDOWN_DONE = False

# Leader lease: un solo SchedulerWorker ejecuta jobs; el resto queda paused
# pero sigue pudiendo add_job sobre el RedisJobStore compartido (APScheduler 3).
SCHEDULER_LEADER_KEY = 'OML:HA:SCHEDULER:LEADER'
SCHEDULER_LEADER_TTL_SEC = 15
SCHEDULER_LEADER_RENEW_SEC = max(1, SCHEDULER_LEADER_TTL_SEC // 3)

_LEASE_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""

_LEASE_RENEW_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
else
  return 0
end
"""

_SCHED_LEASE_THREAD = None
_SCHED_LEASE_STOP = threading.Event()
_SCHED_LEASE_TOKEN = None
_SCHED_IS_LEADER = False
_SCHED_LEASE_LOCK = threading.Lock()


def _stop_scheduler_leader_lease():
    """Detiene el hilo de lease y libera el token si somos líderes."""
    global _SCHED_LEASE_THREAD, _SCHED_LEASE_TOKEN, _SCHED_IS_LEADER
    _SCHED_LEASE_STOP.set()
    thread = _SCHED_LEASE_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=SCHEDULER_LEADER_RENEW_SEC + 1.0)
    _SCHED_LEASE_THREAD = None

    token = _SCHED_LEASE_TOKEN
    if not token:
        _SCHED_IS_LEADER = False
        return
    try:
        sw = globals().get("SchedulerWorker")
        if sw is not None:
            sw._lease_release(token)
    except Exception:
        logger.debug("Scheduler lease release on shutdown failed", exc_info=True)
    _SCHED_LEASE_TOKEN = None
    _SCHED_IS_LEADER = False


def _shutdown_scheduler_gracefully():
    """
    Cierra el scheduler sin bloquear para evitar warnings al terminar el proceso.

    - Evita NameError si SchedulerWorker todavía no existe (signal temprano durante import).
    """
    global _SCHED_SHUTDOWN_DONE
    if _SCHED_SHUTDOWN_DONE:
        return
    _SCHED_SHUTDOWN_DONE = True

    try:
        _stop_scheduler_leader_lease()
        sw = globals().get("SchedulerWorker")  # puede no existir aún
        sched = getattr(sw, "SCHEDULER", None) if sw else None
        if sched is None:
            logger.debug("Scheduler shutdown: SchedulerWorker/SCHEDULER no disponible aún.")
            return

        # Verificar si el scheduler está corriendo antes de hacer shutdown
        if sched.running:
            sched.shutdown(wait=False)
            logger.info("Scheduler shutdown solicitado (wait=False)")
        else:
            logger.debug("Scheduler shutdown: scheduler no está corriendo, omitiendo shutdown.")
    except Exception as e:
        logger.debug("Scheduler shutdown: %s", e, exc_info=True)


def _sched_sig_handler(signum, frame):
    """Ejecuta shutdown en un hilo daemon para no bloquear el signal handler."""
    try:
        threading.Thread(target=_shutdown_scheduler_gracefully, daemon=True).start()
    except Exception:
        # último recurso: evitar que un error aquí tumbe el proceso
        try:
            logger.debug("SIG handler: no se pudo iniciar hilo para shutdown.", exc_info=True)
        except Exception:
            pass


# Registrar hooks de salida (hazlo una sola vez por módulo)
atexit.register(_shutdown_scheduler_gracefully)
signal.signal(signal.SIGTERM, _sched_sig_handler)
signal.signal(signal.SIGINT, _sched_sig_handler)

REDIS_OML_SERVER = os.getenv('REDIS_OML_SERVER', 'oml-redis')

REDIS_OML_PORT = os.getenv('REDIS_OML_PORT', '6379')

REDIS_DIALER_DB = int(os.getenv('REDIS_DIALER_DB', 3))

POSTGRES_OML_SERVER = os.getenv('POSTGRES_OML_SERVER', 'oml-postgres')

POSTGRES_OML_PORT = os.getenv('POSTGRES_OML_PORT', '5432')

POSTGRES_OML_PASSWORD = os.getenv('POSTGRES_OML_PASSWORD')

POSTGRES_OML_USER = os.getenv('POSTGRES_OML_USER', 'omnileads')

POSTGRES_OML_DB = os.getenv('POSTGRES_OML_DB', 'omnileads')

POSTGRES_DIALER_SERVER = os.getenv('POSTGRES_DIALER_SERVER', 'dialer-postgres')

POSTGRES_DIALER_PORT = os.getenv('POSTGRES_DIALER_PORT', '5433')

POSTGRES_DIALER_USER = os.getenv('POSTGRES_DIALER_USER', 'omnidialer')

POSTGRES_DIALER_DB = os.getenv('POSTGRES_DIALER_DB', 'omnidialer')

POSTGRES_DIALER_PASSWORD = os.getenv('POSTGRES_DIALER_PASSWORD')

DIALER_ACD_HOST = os.getenv('DIALER_ACD_HOST', 'omlacd')

DIALER_DIALPLAN_CONTEXT = os.getenv('DIALER_DIALPLAN_CONTEXT', 'oml-dial-dialer')

SCHEDULER_API_HOST = os.getenv('SCHEDULER_API_HOST', 'scheduler-api')

WEEK_DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

CAPS = int(os.getenv('CAPS', 3))

# campaign status possible values
CREATED = 1                     # ESTADO_INACTIVA in OML
ACTIVE = 2                      # ESTADO_ACTIVA in OML
PAUSED = 5                      # ESTADO_PAUSADA in OML
FINALIZED = 3                   # ESTADO_FINALIZADA in OML

CAMPAIGN_STATUS_TO_NAME = {
    1: "INACTIVE",
    2: "ACTIVE",
    5: "PAUSED",
    3: "FINALIZED"
}

AVAILABLE_NEXT_STATUSES = {
    CREATED: [ACTIVE, FINALIZED],
    ACTIVE: [PAUSED, FINALIZED],
    PAUSED: [ACTIVE, FINALIZED],
    FINALIZED: [ACTIVE]
}

# contact status, in sync with OML's incidence_rules statuses
# TODO: see the remaining statuses
STATUS_CREATED = 12
STATUS_SELECTED_CALL = 15
STATUS_ANSWERED_AGENT = 6
STATUS_ANSWERED_PSTN = 7
STATUS_BUSY = 1
STATUS_NOANSWER = 3
STATUS_CONGESTION = 4
STATUS_TERMINATED = 2
STATUS_TIMEOUT = 5
STATUS_CHANUNAVAIL = 8
STATUS_INVALID_NUMBER = 9
STATUS_AMD_MACHINE = 10  # AMD declaró MACHINE (contestador); entidad propia para métricas
STATUS_SHORTCALL = 11  # contestó y colgó en <5s; entidad propia para métricas
STATUS_TEMPORARILY_UNAVAILABLE = 13  # SIP 480; con reglas de incidencia
STATUS_NOT_FOUND = 14  # SIP 404; sin reglas de incidencia
STATUS_EXIT_ABANDON = 16  # PSTN contestó, cliente abandonó cola sin agente
STATUS_EXIT_TIMEOUT = 17  # PSTN contestó, timeout de cola sin agente

NAME_TO_STATUS = {
    "CHANUNAVAIL": STATUS_CHANUNAVAIL,
    "INVALID_NUMBER": STATUS_INVALID_NUMBER,
    "BUSY": STATUS_BUSY,
    "NOANSWER": STATUS_NOANSWER,
    "CONGESTION": STATUS_CONGESTION,
    "ANSWERED_PSTN": STATUS_ANSWERED_PSTN,
    "ANSWERED_AGENT": STATUS_ANSWERED_AGENT,
    "TERMINATED": STATUS_TERMINATED,
    "TIMEOUT": STATUS_TIMEOUT,
    "CANCEL": STATUS_TERMINATED,  # llamada cancelada antes de contestar; sin reglas de incidencia
    "AMD": STATUS_AMD_MACHINE,  # contestador detectado; entidad propia en history y métricas
    "EXIT_SHORTCALL": STATUS_SHORTCALL,  # contestó y colgó en <5s; sin reglas de incidencia
    # Fallo al originar (ACD); sin reglas de incidencia (FAIL_NO_RULES_EVENTS).
    "ORIGINATE_FAILED": STATUS_CHANUNAVAIL,
    "480_TEMPORARILY_UNAVAILABLE": STATUS_TEMPORARILY_UNAVAILABLE,
    "404_NOT_FOUND": STATUS_NOT_FOUND,
    "EXIT_ABANDON": STATUS_EXIT_ABANDON,
    "EXIT_TIMEOUT": STATUS_EXIT_TIMEOUT,
}

# mapeo código -> nombre para interpretar history y métricas
STATUS_TO_NAME = {
    STATUS_BUSY: "BUSY",
    STATUS_TERMINATED: "TERMINATED",
    STATUS_NOANSWER: "NOANSWER",
    STATUS_CONGESTION: "CONGESTION",
    STATUS_TIMEOUT: "TIMEOUT",
    STATUS_ANSWERED_AGENT: "ANSWERED_AGENT",
    STATUS_ANSWERED_PSTN: "ANSWERED_PSTN",
    STATUS_CHANUNAVAIL: "CHANUNAVAIL",
    STATUS_INVALID_NUMBER: "INVALID_NUMBER",
    STATUS_AMD_MACHINE: "AMD",  # AMD Detected / Contestador
    STATUS_SHORTCALL: "EXIT_SHORTCALL",
    STATUS_TEMPORARILY_UNAVAILABLE: "480_TEMPORARILY_UNAVAILABLE",
    STATUS_NOT_FOUND: "404_NOT_FOUND",
    STATUS_EXIT_ABANDON: "EXIT_ABANDON",
    STATUS_EXIT_TIMEOUT: "EXIT_TIMEOUT",
}

# contact final status
INITIAL = 0
PENDING_ATTEMPTS = 1
FINALIZED_NOCONTACT = 2
FINALIZED_SUCCESS = 3

FINAL_STATUS_TO_NAME = {
    FINALIZED_NOCONTACT: "FINALIZED WITH NO CONTACT",
    PENDING_ATTEMPTS: "NO CONTACTS WITH PENDING ATTEMPTS",
    FINALIZED_SUCCESS: "CONTACTED SUCCESSFULLY"
}

NO_DISPOSITION_OPTION = -1
SIN_DISPOSICION_COUNTER_KEY = 'SIN_DISPOSICION'

# percentage called threshold for notify OML
PERCENTAGE_PENDING_CALL_THRESHOLD = 5
RECALC_LOCK_KEY = 'CAMP:DISTRIBUTION:RECALC_LOCK'
RECALC_LOCK_TTL = 2

# status types
PHONE_TYPE = 1
DISPOSITION_TYPE = 2

# fail statuses
# TODO: incorporate the names of the other fail events
FAIL_EVENTS = [
    'BUSY', 'NOANSWER', 'CONGESTION', 'TIMEOUT', 'TERMINATED', 'CHANUNAVAIL',
    'INVALID_NUMBER', 'CANCEL', 'AMD', 'EXIT_SHORTCALL', 'ORIGINATE_FAILED',
    '480_TEMPORARILY_UNAVAILABLE', '404_NOT_FOUND',
    'EXIT_ABANDON', 'EXIT_TIMEOUT',
]

# fail statuses with no incidence rules
FAIL_NO_RULES_EVENTS = [
    'CHANUNAVAIL', 'INVALID_NUMBER', 'CANCEL', 'EXIT_SHORTCALL',
    'ORIGINATE_FAILED', '404_NOT_FOUND', 'EXIT_ABANDON',
]

# Dial status → incidence_rules.status lookup (UI labels).
# History/metrics keep the original event; only rule lookup is aliased.
INCIDENCE_RULE_EVENT_MAP = {
    'AMD': 'TERMINATED',          # UI Contestador
    'EXIT_TIMEOUT': 'TIMEOUT',    # UI Timeout = timeout de cola post-connect
}

# P_hit / Drop — taxonomía canónica (H3)
#
# Hit   = ANSWERED_PSTN (connect humano pre-agente). Entra a P_hit como H.
# Fail  = FAIL_HIT_STATUSES (BUSY, NOANSWER, AMD, …). Entra a P_hit como F.
# Abandon = EXIT_ABANDON / EXIT_TIMEOUT (post-connect, sin agente). Drop only;
#           NO cuenta como Fail ni actualiza P_hit.
#
# Ratios:
#   P_hit_ratio = HIT / (HIT + FAIL)
#   DROP_RATE   = ABANDON / HIT   (CONNECTS_HUMAN == HIT_COUNT == CONNECT_COUNT)
# Otros eventos (ANSWERED_AGENT, EXIT_ANSWERED, EXIT_ACW, ChannelDestroyed, …)
# están fuera de taxonomía: no tocar CAMP:{id}:METRICS.
#
# Ventana: EWMA simétrico (P_HIT, DROP_RATE_EWMA); contadores acumulados + ratios.
P_HIT_ALPHA = 0.1  # EWMA alpha para P_HIT
AMD_TIME = 0.0  # legado; media medida vive en METRICS.AMD_TIME / AMD_LATENCY
HIT_STATUS = 'ANSWERED_PSTN'
# Trunk / originate failures: count TECH_FAIL only — do not move P_HIT EWMA.
TECHNICAL_FAIL_STATUSES = frozenset({
    'ORIGINATE_FAILED', 'CHANUNAVAIL', 'CONGESTION',
    'INVALID_NUMBER', '404_NOT_FOUND', '480_TEMPORARILY_UNAVAILABLE',
})
# Contact-level fails that update P_HIT EWMA toward 0.
FAIL_HIT_STATUSES = frozenset({
    'BUSY', 'NOANSWER', 'TIMEOUT', 'TERMINATED', 'CANCEL', 'AMD',
    'EXIT_SHORTCALL',
})
ABANDON_STATUSES = frozenset({'EXIT_ABANDON', 'EXIT_TIMEOUT'})

# Re-export predictive pacing knobs (settings/default.py via env).
# Defaults: MAX_ABANDON_RATE=0.03, WARM_UP_SAMPLE_SIZE=50, PREDICTIVE_TICK_MS=1000,
# HIT_RATE_FLOOR=0.05, DROP_RATE_ALPHA=0.1 (fuente de γ; ventana ≈ 2/α−1 connects),
# DEFAULT_ART_SEC=15, P_LIB_REMAINING_EPS=1.0, PREDICTIVE_ENABLED=True,
# GAMMA_THROTTLE_FLOOR=0.2, MAX_DIAL_RATIO=5, PACING_SNAPSHOT_TTL_SEC=30,
# THROTTLE_STREAK_SEC=5 (fallback K), THROTTLE_MIN_HITS=20,
# THROTTLE_EXIT_RATIO=0.8, THROTTLE_STATE_TTL_SEC=300, CAPS_REVALIDATE_EVERY=3.

# Throttle kill-switch atómico (wall-clock over_since + latch).
# KEYS: over_since, latch
# ARGV: drop_rate, d_max, exit_thr, streak_sec, ttl, now_ts
# return {over_sec, latched(0|1), force(0|1), event}
_THROTTLE_UPDATE_LUA = """
local over_key = KEYS[1]
local latch_key = KEYS[2]
local d = tonumber(ARGV[1]) or 0
local d_max = tonumber(ARGV[2]) or 0
local exit_thr = tonumber(ARGV[3]) or 0
local streak_sec = tonumber(ARGV[4]) or 5
local ttl = tonumber(ARGV[5]) or 300
local now_ts = tonumber(ARGV[6]) or 0
if streak_sec < 1 then streak_sec = 1 end
local latched = redis.call('EXISTS', latch_key) == 1
local over_since = tonumber(redis.call('GET', over_key))
local function over_sec()
  if over_since == nil then return 0 end
  local elapsed = now_ts - over_since
  if elapsed < 0 then return 0 end
  return math.floor(elapsed)
end
if latched then
  if d < exit_thr then
    redis.call('DEL', latch_key)
    redis.call('DEL', over_key)
    return {0, 0, 0, 'THROTTLE_CLEARED'}
  end
  redis.call('EXPIRE', latch_key, ttl)
  if over_since ~= nil then
    redis.call('EXPIRE', over_key, ttl)
  end
  return {over_sec(), 1, 1, ''}
end
if d_max > 0 and d >= d_max then
  if over_since == nil then
    over_since = now_ts
    redis.call('SET', over_key, tostring(now_ts), 'EX', ttl)
  else
    redis.call('EXPIRE', over_key, ttl)
  end
  local elapsed = now_ts - over_since
  if elapsed < 0 then elapsed = 0 end
  if elapsed >= streak_sec then
    redis.call('SET', latch_key, '1', 'EX', ttl)
    return {math.floor(elapsed), 1, 1, 'THROTTLE_ENGAGED'}
  end
  return {math.floor(elapsed), 0, 0, ''}
end
redis.call('DEL', over_key)
return {0, 0, 0, ''}
"""

# Busy statuses that can contribute to A_expected / P_lib (H4).
BUSY_LIBERATION_STATUSES = frozenset({'ONCALL', 'POSTCALL', 'PAUSE-ACW'})

# incidence rules multinum behauviour
FIXED = 1
MULT = 2

JOB_STARTED = 1
JOB_FAILED = 2

CALLS_DECR_DEDUP_TTL_SEC = int(os.getenv('DIALER_CALLS_DECR_DEDUP_TTL_SEC', '3600'))
# Marker contacto tras cualquier DECR exitoso (TTL corto): cruza Dial AMD/fail
# con callid ↔ ChannelDestroyed sin/otro callid. Reserve limpia al reintentar.
CALLS_CONTACT_DECR_MARKER_TTL_SEC = int(
    os.getenv('DIALER_CALLS_CONTACT_DECR_MARKER_TTL_SEC', '120'),
)
PROCESS_CAMPAIGN_LOCK_TTL_SEC = int(os.getenv('DIALER_PROCESS_CAMPAIGN_LOCK_TTL_SEC', '60'))
AUDIT_ACTIVE_CHANNELS_JOB = 'audit-active-channels'
AUDIT_LOCK_KEY = 'OML:CALLS:AUDIT:LOCK'
ACD_NODES_SET_KEY = 'OML:ACD:NODES'
AUDIT_STREAK_DIR_UP = 'up'
AUDIT_STREAK_DIR_DOWN = 'down'
# Lua: liberar el lock solo si el token sigue siendo el nuestro
_AUDIT_UNLOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""

# Actualiza ATT_SUM / ATT_COUNT / ATT (promedio) de forma atómica en CAMP:{id}:ATT
_UPDATE_CAMPAIGN_ATT_LUA = """
local key = KEYS[1]
local duration = tonumber(ARGV[1]) or 0
local sum = tonumber(redis.call('HINCRBYFLOAT', key, 'ATT_SUM', duration))
local count = tonumber(redis.call('HINCRBY', key, 'ATT_COUNT', 1))
local avg = 0
if count > 0 then
  avg = sum / count
end
redis.call('HSET', key, 'ATT', avg)
return {tostring(sum), tostring(count), tostring(avg)}
"""

# Actualiza ART_SUM / ART_COUNT / ART (promedio) de forma atómica en CAMP:{id}:ART
_UPDATE_CAMPAIGN_ART_LUA = """
local key = KEYS[1]
local duration = tonumber(ARGV[1]) or 0
local sum = tonumber(redis.call('HINCRBYFLOAT', key, 'ART_SUM', duration))
local count = tonumber(redis.call('HINCRBY', key, 'ART_COUNT', 1))
local avg = 0
if count > 0 then
  avg = sum / count
end
redis.call('HSET', key, 'ART', avg)
return {tostring(sum), tostring(count), tostring(avg)}
"""

# Actualiza AMD_SUM / AMD_COUNT / AMD y METRICS.AMD_TIME (H7)
# KEYS[1]=CAMP:{id}:AMD_LATENCY  KEYS[2]=CAMP:{id}:METRICS
_UPDATE_CAMPAIGN_AMD_LATENCY_LUA = """
local lat_key = KEYS[1]
local metrics_key = KEYS[2]
local duration = tonumber(ARGV[1]) or 0
local sum = tonumber(redis.call('HINCRBYFLOAT', lat_key, 'AMD_SUM', duration))
local count = tonumber(redis.call('HINCRBY', lat_key, 'AMD_COUNT', 1))
local avg = 0
if count > 0 then
  avg = sum / count
end
redis.call('HSET', lat_key, 'AMD', avg)
redis.call('HSET', metrics_key, 'AMD_TIME', tostring(avg))
return {tostring(sum), tostring(count), tostring(avg)}
"""

# Actualiza ACW_SUM / ACW_COUNT / ACW (promedio) de forma atómica en CAMP:{id}:ACW
_UPDATE_CAMPAIGN_ACW_LUA = """
local key = KEYS[1]
local duration = tonumber(ARGV[1]) or 0
local sum = tonumber(redis.call('HINCRBYFLOAT', key, 'ACW_SUM', duration))
local count = tonumber(redis.call('HINCRBY', key, 'ACW_COUNT', 1))
local avg = 0
if count > 0 then
  avg = sum / count
end
redis.call('HSET', key, 'ACW', avg)
return {tostring(sum), tostring(count), tostring(avg)}
"""

# Actualiza METRICS: P_HIT (EWMA), P_HIT_RATIO, DROP_RATE, DROP_RATE_EWMA + counts.
# ARGV: hit_flag (1|0), abandon_flag (1|0), p_hit_alpha, drop_alpha, tech_fail (1|0)
# Semántica:
#   hit=1 → HIT/CONNECT++; P_HIT EWMA←1; DROP_RATE_EWMA←0 (decae)
#   tech_fail=1 → TECH_FAIL_COUNT++; no toca P_HIT / FAIL / DROP_EWMA
#   hit=0, abandon=0, tech=0 → FAIL++; P_HIT EWMA←0; no toca DROP_RATE_EWMA
#   abandon=1 → ABANDON++; no toca P_HIT/FAIL; DROP_RATE_EWMA←1
# DROP_RATE = ABANDON / HIT — reporting acumulado
# DROP_RATE_EWMA — fuente canónica para γ
_UPDATE_CAMPAIGN_HIT_LUA = """
local key = KEYS[1]
local hit_flag = tonumber(ARGV[1]) or 0
local abandon_flag = tonumber(ARGV[2]) or 0
local alpha = tonumber(ARGV[3]) or 0.1
local drop_alpha = tonumber(ARGV[4]) or 0.1
local tech_fail = tonumber(ARGV[5]) or 0

if abandon_flag == 1 then
  redis.call('HINCRBY', key, 'ABANDON_COUNT', 1)
  local prev_d = tonumber(redis.call('HGET', key, 'DROP_RATE_EWMA')) or 0
  local drop_ewma = prev_d + drop_alpha * (1 - prev_d)
  redis.call('HSET', key, 'DROP_RATE_EWMA', tostring(drop_ewma))
elseif tech_fail == 1 then
  redis.call('HINCRBY', key, 'TECH_FAIL_COUNT', 1)
elseif hit_flag == 1 then
  redis.call('HINCRBY', key, 'HIT_COUNT', 1)
  redis.call('HINCRBY', key, 'CONNECT_COUNT', 1)
  local prev = tonumber(redis.call('HGET', key, 'P_HIT')) or 0
  local p_hit = prev + alpha * (1 - prev)
  redis.call('HSET', key, 'P_HIT', tostring(p_hit))
  local prev_d = tonumber(redis.call('HGET', key, 'DROP_RATE_EWMA')) or 0
  local drop_ewma = prev_d + drop_alpha * (0 - prev_d)
  redis.call('HSET', key, 'DROP_RATE_EWMA', tostring(drop_ewma))
else
  redis.call('HINCRBY', key, 'FAIL_COUNT', 1)
  local prev = tonumber(redis.call('HGET', key, 'P_HIT')) or 0
  local p_hit = prev + alpha * (0 - prev)
  redis.call('HSET', key, 'P_HIT', tostring(p_hit))
end

local hits = tonumber(redis.call('HGET', key, 'HIT_COUNT')) or 0
local fails = tonumber(redis.call('HGET', key, 'FAIL_COUNT')) or 0
local abandons = tonumber(redis.call('HGET', key, 'ABANDON_COUNT')) or 0
local tech = tonumber(redis.call('HGET', key, 'TECH_FAIL_COUNT')) or 0
local p_hit_ratio = 0
if (hits + fails) > 0 then
  p_hit_ratio = hits / (hits + fails)
end
redis.call('HSET', key, 'P_HIT_RATIO', tostring(p_hit_ratio))

local drop_rate = 0
if hits > 0 then
  drop_rate = abandons / hits
end
redis.call('HSET', key, 'DROP_RATE', tostring(drop_rate))
redis.call('HSET', key, 'WINDOW_MODE', 'ewma_symmetric')
return {
  tostring(tonumber(redis.call('HGET', key, 'P_HIT')) or 0),
  tostring(p_hit_ratio),
  tostring(drop_rate),
  tostring(hits),
  tostring(fails),
  tostring(abandons),
  tostring(tech)
}
"""

# Dial statuses que liberan reserva OML:CALLS (sin esperar ChannelDestroyed)
CALLS_DECR_DIAL_STATUSES = (
    'CANCEL', 'AMD', 'EXIT_SHORTCALL', 'ORIGINATE_FAILED',
    'INVALID_NUMBER', 'CHANUNAVAIL', '480_TEMPORARILY_UNAVAILABLE',
    'NOANSWER', '404_NOT_FOUND', 'EXIT_ABANDON', 'EXIT_TIMEOUT',
    # Early-fail Dial BUSY/CONGESTION/TIMEOUT: ACD may omit ChannelDestroyed.
    'BUSY', 'CONGESTION', 'TIMEOUT',
)



# Fases de canal dialer (CAMP:{id}:CHANNELS + OML:CALLS:PHASE:...)
PHASE_RINGING = 'RINGING'
PHASE_WAITING_AGENT = 'WAITING_AGENT'
PHASE_ONCALL = 'ONCALL'

# Hashes de métricas predictivas (Redis dialer DB3) expuestos en la vista HTMX admin
PREDICTIVE_STATS_HASHES = (
    ('pacing', 'CAMP:{0}:PACING'),
    ('metrics', 'CAMP:{0}:METRICS'),
    ('art', 'CAMP:{0}:ART'),
    ('acw', 'CAMP:{0}:ACW'),
    ('aht', 'CAMP:{0}:AHT'),
    ('amd_latency', 'CAMP:{0}:AMD_LATENCY'),
    ('channels', 'CAMP:{0}:CHANNELS'),
)
CHANNEL_PHASES = (PHASE_RINGING, PHASE_WAITING_AGENT, PHASE_ONCALL)
CHANNEL_PHASE_RANK = {
    PHASE_RINGING: 1,
    PHASE_WAITING_AGENT: 2,
    PHASE_ONCALL: 3,
}
CALLS_PHASE_TTL_SEC = int(os.getenv('DIALER_CALLS_PHASE_TTL_SEC', str(4 * 3600)))

# Reserva atómica: INCR total + check max + HINCR RINGING + SET phase + reserve_ts
# KEYS: calls, channels, phase_contact, reserve_ts
# ARGV: max_channels, phase, reserve_ts, reserve_ttl, phase_ttl
# return {ok(0|1), current_total, reason}
# reason: ok | phase_exists | max_channels
# Idempotente: si ya hay phase del contacto, no INCR (evita doble reserva).
_RESERVE_CHANNEL_LUA = """
local existing = redis.call('GET', KEYS[3])
if existing and existing ~= false and existing ~= '' then
  local cur = tonumber(redis.call('GET', KEYS[1])) or 0
  return {0, cur, 'phase_exists'}
end
local current = tonumber(redis.call('INCR', KEYS[1]))
local maxch = tonumber(ARGV[1]) or 0
if current > maxch then
  redis.call('DECR', KEYS[1])
  return {0, current - 1, 'max_channels'}
end
redis.call('HINCRBY', KEYS[2], ARGV[2], 1)
redis.call('SET', KEYS[3], ARGV[2], 'EX', tonumber(ARGV[5]) or 14400)
redis.call('SET', KEYS[4], ARGV[3], 'EX', tonumber(ARGV[4]) or 120)
return {1, current, 'ok'}
"""

RESERVE_OK = 'ok'
RESERVE_PHASE_EXISTS = 'phase_exists'
RESERVE_MAX_CHANNELS = 'max_channels'
RESERVE_ERROR = 'error'

# Transición forward-only de fase (adopción si no hay phase key).
# KEYS: channels, phase_contact, phase_callid (puede coincidir con contact)
# ARGV: target_phase, phase_ttl
# return {changed(0|1), reason, phase}
_PHASE_TRANSITION_LUA = """
local ranks = {}
ranks['RINGING'] = 1
ranks['WAITING_AGENT'] = 2
ranks['ONCALL'] = 3
local target = ARGV[1]
local target_rank = ranks[target]
if not target_rank then
  return {0, 'invalid', ''}
end
local ttl = tonumber(ARGV[2]) or 14400
local current = redis.call('GET', KEYS[3])
if (not current or current == false) and KEYS[2] ~= KEYS[3] then
  current = redis.call('GET', KEYS[2])
end
if not current or current == false then
  -- Sin reserva/phase viva: no adoptar (evita ONCALL/WAITING fantasma).
  return {0, 'no_reserve', ''}
end
local cur_rank = ranks[current] or 0
if cur_rank >= target_rank then
  -- Solo EXPIRE: un SET aquí puede resucitar la phase key si finalize
  -- ya hizo DEL entre nuestro GET y este write (race → ONCALL huérfano
  -- con DECR dedup ya seteado y hash en 0).
  if redis.call('EXISTS', KEYS[3]) == 1 then
    redis.call('EXPIRE', KEYS[3], ttl)
  end
  if KEYS[2] ~= KEYS[3] then
    redis.call('DEL', KEYS[2])
  end
  return {0, 'noop', current}
end
local bv = tonumber(redis.call('HINCRBY', KEYS[1], current, -1))
if bv < 0 then
  redis.call('HSET', KEYS[1], current, 0)
end
redis.call('HINCRBY', KEYS[1], target, 1)
redis.call('SET', KEYS[3], target, 'EX', ttl)
if KEYS[2] ~= KEYS[3] then
  redis.call('DEL', KEYS[2])
end
return {1, 'ok', target}
"""

# Finaliza canal: dedup + DECR total (piso 0) + HDECR fase persistida + DEL phase keys.
# KEYS: calls, channels, phase_contact, phase_callid, dedup
# ARGV: use_dedup(0|1), dedup_ttl
# return {ok(0|1), orphan_or_reason, total, phase}
_FINALIZE_CHANNEL_LUA = """
if tonumber(ARGV[1]) == 1 then
  local setok = redis.call('SET', KEYS[5], '1', 'NX', 'EX', tonumber(ARGV[2]) or 3600)
  if not setok then
    return {0, 'dup', '', ''}
  end
end
local phase = redis.call('GET', KEYS[4])
if (not phase or phase == false) and KEYS[3] ~= KEYS[4] then
  phase = redis.call('GET', KEYS[3])
end
local val = tonumber(redis.call('DECR', KEYS[1]))
if val < 0 then
  redis.call('SET', KEYS[1], 0)
  val = 0
end
local orphan = 0
if phase and phase ~= false and phase ~= '' then
  local bv = tonumber(redis.call('HINCRBY', KEYS[2], phase, -1))
  if bv < 0 then
    redis.call('HSET', KEYS[2], phase, 0)
  end
else
  orphan = 1
  phase = ''
end
if val == 0 then
  redis.call('HSET', KEYS[2], 'RINGING', 0, 'WAITING_AGENT', 0, 'ONCALL', 0)
elseif orphan == 1 then
  local r = math.max(0, tonumber(redis.call('HGET', KEYS[2], 'RINGING')) or 0)
  local w = math.max(0, tonumber(redis.call('HGET', KEYS[2], 'WAITING_AGENT')) or 0)
  local o = math.max(0, tonumber(redis.call('HGET', KEYS[2], 'ONCALL')) or 0)
  local excess = (r + w + o) - val
  if excess > 0 then
    local dec_r = math.min(r, excess)
    if dec_r > 0 then
      r = r - dec_r
      excess = excess - dec_r
      redis.call('HSET', KEYS[2], 'RINGING', r)
    end
    local dec_w = math.min(w, excess)
    if dec_w > 0 then
      w = w - dec_w
      excess = excess - dec_w
      redis.call('HSET', KEYS[2], 'WAITING_AGENT', w)
    end
    local dec_o = math.min(o, excess)
    if dec_o > 0 then
      o = o - dec_o
      redis.call('HSET', KEYS[2], 'ONCALL', o)
    end
  end
end
redis.call('DEL', KEYS[3])
if KEYS[4] ~= KEYS[3] then
  redis.call('DEL', KEYS[4])
end
return {1, tostring(orphan), tostring(val), tostring(phase)}
"""


class CampaignNotFoundError(Exception):
    """Raised when a campaign is expected to exist in the dialer database but does not."""
    pass


def job_handler_decorator(method):
    def wrapper(*args, **kwargs):
        worker_class = args[0]
        job = args[2]
        id_job = worker_class.insert_job(job)
        if id_job is None:
            # Duplicate Gearman delivery — another worker is already processing this job
            # python-gearman exige bytes para WORK_COMPLETE; devolver None mata
            # el worker y provoca que Gearman vuelva a entregar el mismo job.
            return b'Duplicate job skipped'
        try:
            result = method(*args, **kwargs)
            worker_class.remove_job(id_job)
            return result
        except Exception as e:
            logger.exception(f"An error occurred in {method.__name__}: {e}")
            worker_class.save_job_error(id_job, str(e))
            raise e
    return wrapper


class AverageWorker(DialerWorker):
    """A worker flow with a dialing strategy, call contacts according to the available agents and
    the campaigns they are assigned to"""

    POSTGRES_OML_CONNECTION_STR = (f'postgresql://{POSTGRES_OML_USER}:{POSTGRES_OML_PASSWORD}'
                                   f'@{POSTGRES_OML_SERVER}:{POSTGRES_OML_PORT}/{POSTGRES_OML_DB}')
    POSTGRES_DIALER_CONNECTION_STR = (f'postgresql://{POSTGRES_DIALER_USER}:'
                                      f'{POSTGRES_DIALER_PASSWORD}@{POSTGRES_DIALER_SERVER}:'
                                      f'{POSTGRES_DIALER_PORT}/{POSTGRES_DIALER_DB}')
    REDIS_OML_CONNECTION = None
    REDIS_DIALER_CONNECTION = None
    POSTGRES_OML_POOL = None
    POSTGRES_DIALER_POOL = None
    GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)
    ACTIVE_CAMPAIGNS_SET = 'campaigns:active'
    # H7: cache corta de AmdConf / flag AMD por campaña (Redis OML DB0)
    _amd_conf_fallback_cache = (0.0, None)  # (expires_at, sec_or_None)
    _campaign_amd_cache = {}  # id_campaign -> (expires_at, bool)

    @classmethod
    def _get_gearman_client(cls):
        """
        Helper para obtener el cliente Gearman.
        Retorna el cliente existente o crea uno nuevo si es necesario.

        Returns:
            gearman.GearmanClient: Cliente Gearman configurado
        """
        if cls.GM_CLIENT is None:
            cls.GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)
        return cls.GM_CLIENT

    @classmethod
    def trigger_acd_dial(
        cls, phone_number, campaign_id, contact_id, agent_id=None, attributes=None
    ):
        """
        Envía un job a Gearman para ejecutar una llamada a través del ACD.

        Args:
            phone_number (str): Número de teléfono a llamar
            campaign_id (int): ID de la campaña
            contact_id (int): ID del contacto
            agent_id (int, optional): ID del agente (si aplica)
            attributes (dict, optional): Metadatos adicionales

        Returns:
            bool: True si el job se envió correctamente, False en caso de error
        """
        try:
            # Construir payload JSON
            payload = {
                "command": "dial",
                "number": phone_number,
                "campaign_id": campaign_id,
                "contact_id": contact_id,
                "agent_id": agent_id,
                "metadata": attributes or {}
            }

            message = json.dumps(payload)

            # Obtener cliente Gearman
            client = cls._get_gearman_client()

            # Enviar job a Gearman con background=True
            client.submit_job('acd-call-processor', message, background=True)

            logger.info(
                f"Job enviado a acd-call-processor para contact {contact_id} "
                f"en campaign {campaign_id}, número: {phone_number}"
            )

            return True

        except Exception as e:
            logger.error(
                f"Error al enviar job a acd-call-processor para contact {contact_id} "
                f"en campaign {campaign_id}: {e}",
                exc_info=True
            )
            return False

    # Housekeeping del set de idempotencia (SEEN)
    SEEN_TTL_SECONDS = int(os.getenv("SCHED_SEEN_TTL_SECONDS", 7 * 24 * 3600))  # 7 días

    # Lua: decrementa H[AGENDAS] sólo si > 0, y devuelve el nuevo valor
    _LUA_HINCR_IF_GT0 = """
    local key = KEYS[1]
    local field = ARGV[1]
    local delta = tonumber(ARGV[2])

    local cur = redis.call('HGET', key, field)
    if not cur then
        cur = 0
    else
        cur = tonumber(cur) or 0
    end

    if cur <= 0 then
        return cur
    end

    local newv = cur + delta
    if newv < 0 then newv = 0 end
    redis.call('HSET', key, field, newv)
    return newv
    """

    @classmethod
    def get_oml_connection(cls):
        if cls.POSTGRES_OML_POOL is None:
            cls.POSTGRES_OML_POOL = ConnectionPool(cls.POSTGRES_OML_CONNECTION_STR,
                                                   min_size=1, max_size=2, open=True)
        return cls.POSTGRES_OML_POOL.connection()

    @classmethod
    def get_dialer_connection(cls):
        if cls.POSTGRES_DIALER_POOL is None:
            cls.POSTGRES_DIALER_POOL = ConnectionPool(cls.POSTGRES_DIALER_CONNECTION_STR,
                                                      min_size=1, max_size=2, open=True)
        return cls.POSTGRES_DIALER_POOL.connection()

    @classmethod
    def insert_job(cls, job):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            job_unique = job.unique.decode('utf8')
            job_name = job.task.decode('utf8')
            cursor.execute(
                'INSERT INTO jobs (job_id, job_name, status) VALUES (%s, %s, %s) '
                'ON CONFLICT (job_id) DO NOTHING RETURNING id;',
                (job_unique, job_name, JOB_STARTED))
            row = cursor.fetchone()
            if row is None:
                # Job already being handled by another worker (duplicate delivery from Gearman)
                logger.warning(f"Duplicate job skipped: job_id={job_unique}, job_name={job_name}")
                return None
            return row[0]

    @classmethod
    def save_job_error(cls, id_job, exception):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE jobs SET status = %s, error = %s WHERE id = %s;',
                           (JOB_FAILED, exception, id_job))

    @classmethod
    def remove_job(cls, id_job):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM jobs WHERE id = %s;', (id_job,))

    @classmethod
    def system_is_active(cls):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT is_active FROM system_control;')
            return cursor_dialer.fetchone()[0]

    # --- agendas counter helpers ---
    @classmethod
    def _agendas_counter_key(cls, id_campaign):
        return f'CAMP:{id_campaign}:SCHED'

    @classmethod
    def _agendas_seen_set_key(cls, id_campaign):
        # Para evitar doble decremento por el mismo job_id
        return f'CAMP:{id_campaign}:SCHED:SEEN'

    @classmethod
    def agendas_increment(cls, id_campaign, delta=1):
        cls.connect_redis_dialer()
        key = cls._agendas_counter_key(int(id_campaign))
        try:
            cls.REDIS_DIALER_CONNECTION.hincrby(key, 'AGENDAS', int(delta))
        except Exception:
            logger.exception("agendas_increment failed camp=%s delta=%s", id_campaign, delta)

    @classmethod
    def agendas_decrement(cls, id_campaign, delta=1):
        """
        Decrementa AGENDAS de forma atómica y nunca negativa (sin idempotencia).
        Útil como fallback si algo llama agendas_decrement() directamente.
        """
        cls.connect_redis_dialer()
        key = cls._agendas_counter_key(int(id_campaign))

        try:
            dec = -abs(int(delta or 1))
            cls.REDIS_DIALER_CONNECTION.eval(
                cls._LUA_HINCR_IF_GT0,
                1,          # numkeys
                key,        # KEYS[1]
                "AGENDAS",  # ARGV[1]
                dec         # ARGV[2]
            )
        except Exception:
            logger.exception("agendas_decrement failed camp=%s delta=%s", id_campaign, delta)

    @classmethod
    def rebuild_active_campaigns_set(cls):
        cls.connect_redis_dialer()
        with cls.get_dialer_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM ONLY campaign WHERE dialer_status = %s;", (ACTIVE,))
            active_ids = [str(r[0]) for r in cur.fetchall()]
        pipe = cls.REDIS_DIALER_CONNECTION.pipeline()
        pipe.delete(cls.ACTIVE_CAMPAIGNS_SET)
        if active_ids:
            pipe.sadd(cls.ACTIVE_CAMPAIGNS_SET, *active_ids)
            for cid in active_ids:
                pipe.hset(f'CAMP:{cid}:DISTRIBUTION', 'STATUS', '1')
        pipe.execute()

    @classmethod
    def allowed_calls_prority_percentage(
        cls, id_campaign: int, contacts_attempts_number_prev: int
    ) -> int:
        cls.connect_redis_dialer()

        my_key = f'CAMP:{id_campaign}:DISTRIBUTION'
        cls.REDIS_DIALER_CONNECTION.hset(my_key, 'CALLS', int(contacts_attempts_number_prev))

        if contacts_attempts_number_prev <= 0:
            return 0

        active_ids_raw = cls.REDIS_DIALER_CONNECTION.smembers(cls.ACTIVE_CAMPAIGNS_SET)
        if not active_ids_raw:
            return 0

        active_ids = []
        for cid in active_ids_raw:
            try:
                active_ids.append(int(cid))
            except (TypeError, ValueError):
                logger.warning("ID inválido en %s: %r", cls.ACTIVE_CAMPAIGNS_SET, cid)

        if not active_ids:
            return 0

        keys = [f'CAMP:{cid}:DISTRIBUTION' for cid in active_ids]
        pipe = cls.REDIS_DIALER_CONNECTION.pipeline()
        for key in keys:
            pipe.hget(key, 'STATUS')
            pipe.hget(key, 'CALLS')
            pipe.hget(key, 'PRIORITY')
        flat = pipe.execute()

        total_calls = 0
        demanding_priorities = []
        total_active_priority = 0
        demanding_priority = 0
        for i in range(0, len(flat), 3):
            status = flat[i]
            calls_raw = flat[i + 1]
            prio_raw = flat[i + 2]
            if status == '1':
                try:
                    calls_val = int(calls_raw or 0)
                except (TypeError, ValueError):
                    calls_val = 0
                try:
                    prio_val = max(0, int(prio_raw or 0))
                except (TypeError, ValueError):
                    prio_val = 0
                total_active_priority += prio_val
                if calls_val > 0:
                    total_calls += calls_val
                    demanding_priorities.append(prio_val)
                    demanding_priority += prio_val

        if total_calls <= 0:
            return 0

        # Si solo esta campaña demanda llamadas en este ciclo, o todas las campañas
        # demandantes tienen la misma prioridad (el reparto multi-cola ya pondera
        # agentes por 1/queue_count), no aplicar un segundo recorte por porcentaje.
        if len(demanding_priorities) <= 1 or len(set(demanding_priorities)) == 1:
            return contacts_attempts_number_prev

        pct_raw = cls.REDIS_DIALER_CONNECTION.hget(my_key, 'PERCENTAGE')
        try:
            percentage = float(pct_raw or 0.0)
        except (TypeError, ValueError):
            percentage = 0.0

        if total_active_priority > 0 and 0 < demanding_priority < total_active_priority:
            percentage = min(
                1.0,
                percentage * (float(total_active_priority) / float(demanding_priority)),
            )

        assigned = int(floor(percentage * total_calls))

        if assigned < 1 and contacts_attempts_number_prev > 0:
            assigned = 1

        return min(assigned, contacts_attempts_number_prev)

    @classmethod
    def _extract_campaign_id_from_distribution_key(cls, redis_key):
        if not redis_key:
            return None
        try:
            _, campaign_id, _ = redis_key.split(':', 2)
            return int(campaign_id)
        except (ValueError, AttributeError):
            return None

    @classmethod
    def get_campaign_priority_from_db(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT priority FROM campaign WHERE id = %s;', (id_campaign,))
            result = cursor_dialer.fetchone()
            if result:
                return result[0]
        return None

    @classmethod
    def get_campaign_priority(cls, id_campaign, redis_key=None):
        cls.connect_redis_dialer()
        if redis_key is None:
            redis_key = f'CAMP:{id_campaign}:DISTRIBUTION'
        priority_value = cls.REDIS_DIALER_CONNECTION.hget(redis_key, 'PRIORITY')
        if priority_value is not None:
            return int(priority_value)
        priority_from_db = cls.get_campaign_priority_from_db(id_campaign)
        if priority_from_db is None:
            return None
        cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'PRIORITY', priority_from_db)
        return int(priority_from_db)

    @classmethod
    def _ensure_priority_cached(cls, id_campaign: int, redis_key: str) -> int | None:
        """
        Checks that the PRIORITY key is present in Redis for the campaign.
        If missing or invalid, it backfills the value from the DB and sets it.
        Returns the priority (int) or None on failure.
        """
        pr = cls.REDIS_DIALER_CONNECTION.hget(redis_key, 'PRIORITY')
        if pr is not None:
            try:
                return int(pr)
            except (TypeError, ValueError):
                pass  # invalid value, will backfill

        # Backfill from PGSQL
        pr_db = cls.get_campaign_priority_from_db(id_campaign)
        if pr_db is None or pr_db <= 0:
            logger.warning('Campaña %s sin prioridad válida en DB. Se usará 0.', id_campaign)
            # Set 0 on Redis
            cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'PRIORITY', 0)
            return 0

        cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'PRIORITY', pr_db)
        logger.info('Backfill de prioridad para campaña %s: %d', id_campaign, pr_db)
        return int(pr_db)

    @classmethod
    def update_percentages_priority_campaigns(cls):
        """
        Recalculates the PERCENTAGE for ALL active campaigns atomically and resiliently.
        It relies on the campaigns:active set and uses a distributed lock
        to prevent race conditions. The PERCENTAGE is calculated based on the PRIORITY
        of each campaign.
        """
        cls.connect_redis_dialer()

        lock_key = 'lock:update_percentages'
        if not cls.REDIS_DIALER_CONNECTION.set(lock_key, '1', nx=True, ex=30):
            logger.debug("Recálculo de porcentajes ya en progreso, saltando.")
            return

        try:
            # 1. Get all active campaign IDs
            active_ids_raw = cls.REDIS_DIALER_CONNECTION.smembers(cls.ACTIVE_CAMPAIGNS_SET)
            if not active_ids_raw:
                logger.debug('No active campaigns; skipping percentage update.')
                return

            active_ids = [int(cid) for cid in active_ids_raw if cid.isdigit()]

            # 2. Collect priorities, backfilling if necessary
            campaign_priorities = {}
            total_priority = 0
            for cid in active_ids:
                redis_key = f'CAMP:{cid}:DISTRIBUTION'
                priority = cls._ensure_priority_cached(cid, redis_key)
                # Use 0 if priority is None to participate in uniform distribution
                priority = priority or 0
                campaign_priorities[cid] = priority
                total_priority += priority

            # 3. Calculate and write new percentages in a pipeline
            pipe = cls.REDIS_DIALER_CONNECTION.pipeline()

            # Fallback to uniform distribution if no valid priorities
            if total_priority <= 0:
                logger.warning(
                    'Total priority is 0. Using uniform distribution'
                    ' for %d active campaigns.', len(active_ids)
                )
                if active_ids:
                    uniform_pct = 1.0 / len(active_ids)
                    for cid in active_ids:
                        pipe.hset(f'CAMP:{cid}:DISTRIBUTION', 'PERCENTAGE', uniform_pct)
            else:
                # Normal distribution based on priority
                for cid, priority in campaign_priorities.items():
                    percentage = float(priority) / total_priority
                    pipe.hset(f'CAMP:{cid}:DISTRIBUTION', 'PERCENTAGE', percentage)
            pipe.execute()
            logger.info("Recalculated distribution percentages for %d campaigns.", len(active_ids))

        finally:
            # 4. Release the lock
            cls.REDIS_DIALER_CONNECTION.delete(lock_key)

    @classmethod
    def _process_campaign_lock_key(cls, id_campaign):
        return f'PROCESS-CAMPAIGN-{id_campaign}'

    @classmethod
    def _renew_process_campaign_lock(cls, id_campaign, token):
        """Renueva TTL sólo si el token sigue siendo el dueño del lease."""
        if int(id_campaign or 0) == 0 or not token:
            return False
        try:
            cls.connect_redis_dialer()
            return bool(cls.REDIS_DIALER_CONNECTION.eval(
                _LEASE_RENEW_LUA, 1,
                cls._process_campaign_lock_key(id_campaign),
                token,
                PROCESS_CAMPAIGN_LOCK_TTL_SEC,
            ))
        except Exception:
            logger.debug(
                'Campaign %s: renew process-campaign lock failed', id_campaign,
                exc_info=True,
            )
            return False

    @classmethod
    def _release_process_campaign_lock(cls, id_campaign, token):
        """Libera el lease sólo si el token sigue siendo el nuestro."""
        if int(id_campaign or 0) == 0 or not token:
            return False
        try:
            cls.connect_redis_dialer()
            return bool(cls.REDIS_DIALER_CONNECTION.eval(
                _LEASE_RELEASE_LUA, 1,
                cls._process_campaign_lock_key(id_campaign),
                token,
            ))
        except Exception:
            logger.debug(
                'Campaign %s: release process-campaign lock failed', id_campaign,
                exc_info=True,
            )
            return False

    @classmethod
    def _revert_unprocessed_contacts(cls, id_campaign, remaining_contacts):
        """Devuelve contactos SELECTED_CALL no discados a CREATED al salir del lote."""
        for pending_contact in (remaining_contacts or []):
            try:
                cls._mark_contact_status_created(id_campaign, pending_contact[0])
            except Exception:
                logger.warning(
                    'Campaign %s: failed reverting contact %s to CREATED',
                    id_campaign, pending_contact[0], exc_info=True,
                )

    @classmethod
    def process_campaign_inside(cls, id_campaign, lock_token=None):
        while cls.campaign_is_active(id_campaign):
            cls.connect_redis_dialer()
            if lock_token and not cls._renew_process_campaign_lock(
                    id_campaign, lock_token):
                logger.warning(
                    'Campaign %s: lost PROCESS-CAMPAIGN lease; exiting loop',
                    id_campaign,
                )
                break
            logger.debug(f'\nCampaign {id_campaign} is active')
            allowed_to_call, extra_info = cls.is_allowed_to_call(id_campaign)
            if allowed_to_call:
                logger.debug(f'Campaign {id_campaign} is allowed to call')
                contacts_attempts_number_prev = cls.allowed_parallel_contact_attempts(id_campaign)
                contacts_attempts_number = cls.allowed_calls_prority_percentage(
                    id_campaign, contacts_attempts_number_prev)

                # Conectar Redis antes del loop para reservas atómicas
                cls.connect_redis_dialer()
                campaign_max = cls.get_campaign_max_available_channels(id_campaign)
                active_channels, _phases_snap = cls._get_calls_and_phases(id_campaign)

                # Log de información de canales en el ciclo
                logger.debug(
                    f'Campaign {id_campaign}: ciclo actual - '
                    f'canales_maximos={campaign_max} llamadas_actuales={active_channels} '
                    f'contactos_permitidos={contacts_attempts_number}'
                )

                initial_time = datetime.datetime.now()
                caps_calls_counter = 0
                dials_in_batch = 0
                contacts = (
                    cls.take_contacts(contacts_attempts_number, id_campaign)
                    if contacts_attempts_number > 0
                    else []
                )
                if lock_token and not cls._renew_process_campaign_lock(
                        id_campaign, lock_token):
                    logger.warning(
                        'Campaign %s: lost PROCESS-CAMPAIGN lease after '
                        'take_contacts; reverting %s contacts and exiting',
                        id_campaign, len(contacts),
                    )
                    cls._revert_unprocessed_contacts(id_campaign, contacts)
                    break
                # Recycle aunque OML:CALLS>0: en vuelo = phase keys.
                # CALLS zombi no debe dejar SELECTED sin CREATED eternos.
                if not contacts:
                    recycled = cls._recycle_stuck_selected_if_idle(id_campaign)
                    if recycled and contacts_attempts_number > 0:
                        contacts = cls.take_contacts(
                            contacts_attempts_number, id_campaign)
                    if lock_token and not cls._renew_process_campaign_lock(
                            id_campaign, lock_token):
                        logger.warning(
                            'Campaign %s: lost PROCESS-CAMPAIGN lease after '
                            'recycle; reverting %s contacts and exiting',
                            id_campaign, len(contacts),
                        )
                        cls._revert_unprocessed_contacts(id_campaign, contacts)
                        break

                if not contacts:
                    dial_mode, _ = cls.resolve_dial_mode(id_campaign)
                    if lock_token and not cls._renew_process_campaign_lock(
                            id_campaign, lock_token):
                        logger.warning(
                            'Campaign %s: lost PROCESS-CAMPAIGN lease (idle); '
                            'exiting',
                            id_campaign,
                        )
                        break
                    if (
                        dial_mode == cls.DIAL_MODE_PREDICTIVE
                        and PREDICTIVE_TICK_MS > 0
                    ):
                        sleep(PREDICTIVE_TICK_MS / 1000.0)
                    elif TIME_BETWEEN_CALLS:
                        sleep(max(float(TIME_BETWEEN_CALLS), 0.1))
                lost_lease = False
                abort_batch = False
                revalidate_every = max(1, int(CAPS_REVALIDATE_EVERY or CAPS or 1))
                idx = 0
                contact_handled = True
                try:
                    for idx, contact in enumerate(contacts):
                        contact_handled = False
                        if abort_batch:
                            cls._mark_contact_status_created(id_campaign, contact[0])
                            contact_handled = True
                            continue
                        while True:
                            # we need to ensure the selected contact is eventually called
                            current_time = datetime.datetime.now()
                            current_delta = current_time - initial_time
                            if current_delta >= timedelta(seconds=1):
                                logger.debug(f"Campaign {id_campaign}: CAPS init")
                                caps_calls_counter = 0
                                initial_time = current_time
                                # Cada ventana CAPS: renovar latch + chequear headroom
                                should_abort, abort_reason = cls._caps_batch_should_abort(
                                    id_campaign, campaign_max, dials_done=dials_in_batch,
                                )
                                if should_abort:
                                    logger.info(
                                        'Campaign %s: abort CAPS batch (%s) '
                                        'after %s dials',
                                        id_campaign, abort_reason, dials_in_batch,
                                    )
                                    cls._mark_contact_status_created(
                                        id_campaign, contact[0],
                                    )
                                    contact_handled = True
                                    abort_batch = True
                                    break
                            else:
                                if caps_calls_counter < CAPS:
                                    logger.debug(
                                        f"Campaign {id_campaign}: attempt to call selected contact")

                                    if (
                                        dials_in_batch > 0
                                        and dials_in_batch % revalidate_every == 0
                                    ):
                                        should_abort, abort_reason = (
                                            cls._caps_batch_should_abort(
                                                id_campaign, campaign_max,
                                                dials_done=dials_in_batch,
                                            )
                                        )
                                        if should_abort:
                                            logger.info(
                                                'Campaign %s: abort CAPS batch (%s) '
                                                'after %s dials',
                                                id_campaign, abort_reason,
                                                dials_in_batch,
                                            )
                                            cls._mark_contact_status_created(
                                                id_campaign, contact[0],
                                            )
                                            contact_handled = True
                                            abort_batch = True
                                            break

                                    # P0: techo global originates/s (antes de reservar)
                                    cls.connect_redis_dialer()
                                    caps_ok, caps_reason = try_consume_global_caps(
                                        cls.REDIS_DIALER_CONNECTION,
                                    )
                                    if not caps_ok:
                                        logger.info(
                                            'Campaign %s: abort CAPS batch (%s) '
                                            'after %s dials',
                                            id_campaign,
                                            caps_reason or 'global_caps',
                                            dials_in_batch,
                                        )
                                        cls._mark_contact_status_created(
                                            id_campaign, contact[0],
                                        )
                                        contact_handled = True
                                        abort_batch = True
                                        break

                                    # Reserva ANTES de mandar a Gearman para evitar el lag
                                    reserve_reason = cls._reserve_dialer_channel(
                                        id_campaign, contact[0],
                                    )
                                    if reserve_reason != RESERVE_OK:
                                        # phase viva: no reencolar a CREATED
                                        if reserve_reason != RESERVE_PHASE_EXISTS:
                                            cls._mark_contact_status_created(
                                                id_campaign, contact[0],
                                            )
                                        contact_handled = True
                                        if reserve_reason == RESERVE_MAX_CHANNELS:
                                            abort_batch = True
                                        break

                                    cls.attempt_contact(contact, id_campaign)
                                    contact_handled = True
                                    caps_calls_counter += 1
                                    dials_in_batch += 1
                                    if lock_token and not cls._renew_process_campaign_lock(
                                            id_campaign, lock_token):
                                        logger.warning(
                                            'Campaign %s: lost PROCESS-CAMPAIGN '
                                            'lease mid-dial; exiting',
                                            id_campaign,
                                        )
                                        lost_lease = True
                                    break
                                else:
                                    logger.debug(f"Campaign {id_campaign}: CAPS sleep")
                                    remaining = (timedelta(seconds=1) - current_delta).total_seconds()
                                    if lock_token and not cls._renew_process_campaign_lock(
                                            id_campaign, lock_token):
                                        logger.warning(
                                            'Campaign %s: lost PROCESS-CAMPAIGN '
                                            'lease during CAPS; exiting',
                                            id_campaign,
                                        )
                                        lost_lease = True
                                        break
                                    sleep(remaining)
                        if lost_lease:
                            break
                finally:
                    if contacts:
                        unhandled_start = idx + 1 if contact_handled else idx
                        if unhandled_start < len(contacts):
                            cls._revert_unprocessed_contacts(
                                id_campaign, contacts[unhandled_start:],
                            )
                if lost_lease:
                    break
            else:
                # schedule process-campaign for the next time the campaign is allowed to run
                next_allowed_date = cls.get_next_allowed_date(id_campaign, extra_info)
                message = json.dumps({
                    'datetime_start': next_allowed_date.strftime('%d/%m/%y %H:%M:%S'),
                    'type': 'process-campaign',
                    'id_campaign': str(id_campaign),
                })
                cls.GM_CLIENT.submit_job('schedule-agenda', message)
                return None

    @classmethod
    def get_next_day_of_week_allowed(
            cls, permission_days_campaign, day_of_week, current_date, hour_start):
        # get next day of the week allowed in the campaign
        # search for an allowed day
        dow = (day_of_week + 1) % 7
        while not permission_days_campaign[dow]:
            dow = (dow + 1) % 7
        # construct the datetime
        days_until_next_day_allowed = (dow - day_of_week) % 7
        date = current_date + timedelta(days_until_next_day_allowed)
        return datetime.datetime.combine(date, hour_start)

    @classmethod
    def get_next_allowed_date(cls, id_campaign, extra_info):
        (day_of_week_allowed, day_of_week, hour_match, current_date, hour,
         minute, campaign_info) = extra_info
        (hour_start, __, monday, tuesday, wednesday, thursday, friday, saturday,
         sunday) = campaign_info
        permission_days_campaign = campaign_info[2:]
        # if the day of the week is not allowed get the next day of week allowed with hour_start
        if not day_of_week_allowed:
            return cls.get_next_day_of_week_allowed(
                permission_days_campaign, day_of_week, current_date, hour_start)
        # if current time < hour_start, just use the same day with hour_start
        # else, get the next day of week allowed with hour start
        current_time = datetime.time(hour, minute)
        if current_time < hour_start:
            return datetime.datetime.combine(current_date, hour_start)
        return cls.get_next_day_of_week_allowed(
            permission_days_campaign, day_of_week, current_date, hour_start)

    @classmethod
    def connect_redis_oml(cls):
        if cls.REDIS_OML_CONNECTION is None:
            cls.REDIS_OML_CONNECTION = redis.Redis(
                host=REDIS_OML_SERVER, port=REDIS_OML_PORT, decode_responses=True, db=0)


    @classmethod
    def _notify_dialer_event(cls, payload):
        """XADD to OML:STREAM:DIALER (DB 2); optional dual-write PUBLISH."""
        cls.connect_redis_oml()
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode('utf-8')
        if isinstance(payload, str):
            payload = json.loads(payload)
        notify_dialer_event(payload, publish_conn=cls.REDIS_OML_CONNECTION)

    @classmethod
    def connect_redis_dialer(cls):
        if cls.REDIS_DIALER_CONNECTION is None:
            cls.REDIS_DIALER_CONNECTION = redis.Redis(
                host=REDIS_DIALER_SERVER,
                port=int(REDIS_DIALER_PORT),
                decode_responses=True,
                db=REDIS_DIALER_DB,
            )

    @classmethod
    def is_allowed_to_call(cls, id_campaign):
        # check if opening hours are ok
        # TODO: a possible optimization here could be pause the campaign and place a scheduled task
        # to resume it later at the following allowed opening hour
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            return cls.opening_hours_match(cursor_dialer, id_campaign)

    @classmethod
    def get_campaign_data(cls, id_campaign, cursor_oml, contact_strategy):
        """
        Obtiene los datos de la campaña desde OML con retry automático y refresco de transacción.
        """
        logger.debug(f"Retrieving data from OML campaign with id={id_campaign}")

        # Definimos la función helper DENTRO del método (sin @classmethod)
        def execute_with_retry(query, params, context, max_retries=10, initial_delay=0.5):
            """Ejecuta una consulta con retry y REFRESCANDO la conexión."""
            for attempt in range(max_retries):
                # --- AQUÍ ESTÁ LA CLAVE DEL ARREGLO ---
                # Si fallamos la primera vez, hacemos commit en la conexión de LECTURA
                # para forzar a Postgres a refrescar el snapshot y ver datos nuevos.
                if attempt > 0:
                    try:
                        cursor_oml.connection.commit()
                    except Exception as e:
                        logger.warning(f"Error refreshing connection snapshot: {e}")
                # --------------------------------------

                cursor_oml.execute(query, params)
                row = cursor_oml.fetchone()

                if row is not None:
                    if attempt > 0:
                        logger.debug(
                            f"Campaign {id_campaign}: Successfully retrieved {context} "
                            f"on attempt {attempt + 1}"
                        )
                    return row

                if attempt < max_retries - 1:
                    # Usamos min() para que la espera no sea eterna (tope 3 seg)
                    delay = min(initial_delay * (2 ** attempt), 3.0)
                    logger.warning(
                        f"Campaign {id_campaign}: No row returned for {context} "
                        f"on attempt {attempt + 1}/{max_retries}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    sleep(delay)
                else:
                    raise ValueError(
                        f"Campaign {id_campaign}: no row returned for {context} "
                        f"after {max_retries} attempts"
                    )

        # 1. ominicontacto_app_campana
        logger.debug(f"Campaign {id_campaign}: from ominicontacto_app_campana")
        campaign_id_data = execute_with_retry(
            "SELECT id,estado,nombre,fecha_inicio,fecha_fin,control_de_duplicados,prioridad "
            "FROM ominicontacto_app_campana WHERE id = %s;",
            (id_campaign,),
            "ominicontacto_app_campana"
        )

        # 2. queue_table
        logger.debug(f"Campaign {id_campaign}: from queue_table")
        campaign_id_data += execute_with_retry(
            "SELECT strategy,wait,initial_predictive_model,initial_boost_factor,maxlen "
            "FROM queue_table WHERE campana_id = %s;",
            (id_campaign,),
            "queue_table"
        )

        # 3. ominicontacto_app_actuacionvigente
        logger.debug(f"Campaign {id_campaign}: from ominicontacto_app_actuacionvigente")
        campaign_id_data += execute_with_retry(
            "SELECT domingo,lunes,martes,miercoles,jueves,viernes,sabado,hora_desde,hora_hasta "
            "FROM ominicontacto_app_actuacionvigente WHERE campana_id = %s;",
            (id_campaign,),
            "ominicontacto_app_actuacionvigente"
        )

        logger.debug(f"Campaign {id_campaign}: setting dialer specific options")
        campaign_id_data += (contact_strategy, CREATED)

        # Las reglas de incidencia usualmente se crean en la misma transacción.
        # Si fallan, el retry debería aplicarse también, pero por ahora
        # mantenemos tu estructura original asumiendo que si la campaña apareció,
        # las reglas también.
        logger.debug(f"Campaign {id_campaign}: from incidence rules")
        cursor_oml.execute(
            "SELECT * FROM ominicontacto_app_reglasincidencia WHERE campana_id = %s;",
            (id_campaign,)
        )
        incidence_rules_data = cursor_oml.fetchall()

        logger.debug(f"Campaign {id_campaign}: from incidence rules for disposition options")
        cursor_oml.execute(
            """SELECT ric.id,ric.opcion_calificacion_id,ric.intento_max,ric.reintentar_tarde,
            ric.en_modo,opc.campana_id
            FROM ominicontacto_app_reglaincidenciaporcalificacion AS ric
            INNER JOIN ominicontacto_app_opcioncalificacion AS opc ON
            ric.opcion_calificacion_id = opc.id
            AND opc.campana_id = %s;""", (id_campaign,)
        )
        incidence_rules_disposition_data = cursor_oml.fetchall()

        # 4. Metadata (También aplicamos retry aquí por seguridad)
        metadata_row = execute_with_retry(
            """SELECT db.metadata FROM ominicontacto_app_basedatoscontacto as db
            INNER JOIN ominicontacto_app_campana AS ca ON ca.bd_contacto_id = db.id
            AND ca.id = %s;""",
            (id_campaign,),
            "basedatoscontacto.metadata join (bd_contacto_id)"
        )
        metadata = json.dumps(metadata_row[0])
        campaign_id_data += (metadata,)

        cls.connect_redis_oml()
        customdialerdst = cls.REDIS_OML_CONNECTION.hget(
            f"OML:CAMP:{id_campaign}", "CUSTOMDIALERDST"
        )
        campaign_id_data += (customdialerdst,)

        return campaign_id_data, incidence_rules_data, incidence_rules_disposition_data

    @classmethod
    @job_handler_decorator
    def edit_campaign(cls, worker, job):
        # assumes the dialer campaign exists in OML with all the required tables and fields created
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Editing the campaign {id_campaign}')
        contact_strategy = data['contact_strategy']
        # 0- pause campaign
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                orig_status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)
                cls.set_campaign_status(id_campaign, PAUSED, cursor_dialer)
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    (campaign_id_data, incidence_rules_data,
                     incidence_rules_disposition_data) = cls.get_campaign_data(
                        id_campaign, cursor_oml, contact_strategy)
                    priority = campaign_id_data[6]
                    # 1- update campaign table
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the campaign data into omnidialer')
                    cls.connect_redis_dialer()
                    customdialerdst = campaign_id_data[-1]
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:CUSTOMDIALERDST', customdialerdst)
                    voicebot = cls.REDIS_OML_CONNECTION.hget(
                        f"OML:CAMP:{id_campaign}", "VOICEBOT")
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:VOICEBOT', voicebot or 'False')
                    # campaign_id_data[9] = initial_predictive_model (from queue_table)
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:PREDICTIVE_MODEL',
                        'True' if campaign_id_data[9] else 'False')
                    params = campaign_id_data[1:] + (id_campaign,)
                    cursor_dialer.execute(
                        """UPDATE campaign SET oml_status = %s, name = %s, start_date = %s,
                        end_date = %s, duplicates_control = %s, priority = %s, strategy = %s,
                        wait = %s, initial_predictive_model = %s, initial_boost_factor = %s,
                        max_channels = %s, sunday = %s, monday = %s, tuesday = %s, wednesday = %s,
                        thursday = %s, friday = %s, saturday = %s, hour_start = %s, hour_ends = %s,
                        contact_strategy = %s, dialer_status = %s, metadata = %s,
                        customdialerdst = %s
                        WHERE id = %s;""", params)
                    # 2- update incidence rules
                    cursor_dialer.execute('DELETE FROM incidence_rules WHERE campaign_id = %s',
                                          (id_campaign,))
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules into omnidialer')
                    for incidence_rule in incidence_rules_data:
                        cursor_dialer.execute("""INSERT INTO incidence_rules
                         (id, status, status_custom, max_attempt, retry_later, in_mode, campaign_id)
                         VALUES (%s, %s, %s, %s, %s, %s, %s);""", incidence_rule)
                    cursor_dialer.execute("""DELETE FROM incidence_rules_disposition
                    WHERE campaign_id = %s""", (id_campaign,))
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules for disposition'
                        ' into omnidialer')
                    for incidence_rule in incidence_rules_disposition_data:
                        cursor_dialer.execute("""INSERT INTO incidence_rules_disposition
                        (id, disposition_option_id, max_attempt, retry_later, in_mode, campaign_id)
                        VALUES (%s, %s, %s, %s, %s, %s);""", incidence_rule)
                if orig_status_campaign == ACTIVE:
                    cls.set_campaign_status(id_campaign, ACTIVE, cursor_dialer)
        try:
            cls.get_boost_factor.cache_clear()
            cls.get_predictive_model.cache_clear()
            cls.get_campaign_max_available_channels.cache_clear()
            cls.get_incidence_rule.cache_clear()
            cls.get_incidence_rule_disposition.cache_clear()
            cls.get_opening_hours.cache_clear()
            cls.get_prefix.cache_clear()
        except AttributeError:
            pass

        cls.REDIS_DIALER_CONNECTION.hset(
            f'CAMP:{id_campaign}:DISTRIBUTION', 'PRIORITY', priority)

        cls.update_percentages_priority_campaigns()

        if orig_status_campaign == ACTIVE:
            message = json.dumps({'id_campaign': id_campaign})
            cls.GM_CLIENT.submit_job('process-campaign', message, background=True)

        response = f'Campaign {id_campaign} with strategy {contact_strategy} succesfully updated!!!'

        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    def get_contacts_campaign(cls, cursor, size):
        return cursor.fetchmany(size=size)

    @classmethod
    def copy_contacts_from_oml(cls, cursor_dialer, cursor_oml, id_campaign):
        logger.debug(f'Campaign {id_campaign}: retrieving the contacts')
        cursor_oml.execute(
            'SELECT barajar_contactos FROM ominicontacto_app_campana WHERE id = %s;',
            (id_campaign,))
        shuffle_row = cursor_oml.fetchone()
        shuffle = bool(shuffle_row[0]) if shuffle_row is not None else False
        sql = """SELECT co.id, co.telefono, co.datos, co.es_originario
        FROM ominicontacto_app_contacto AS co
        INNER JOIN ominicontacto_app_basedatoscontacto AS db ON
        db.id = co.bd_contacto_id INNER JOIN ominicontacto_app_campana
        AS ca ON db.id = ca.bd_contacto_id AND ca.id = %s"""
        if shuffle:
            sql += ' ORDER BY random()'
            logger.debug(f'Campaign {id_campaign}: shuffling contacts on load')
        sql += ';'
        size = 1000
        logger.debug(f'Campaign {id_campaign}: copying the contacts')
        cursor_oml.execute(sql, (id_campaign,))
        while True:
            contacts = cls.get_contacts_campaign(cursor_oml, size)
            if not contacts:
                break
            for (id_contact, phone, data, is_original) in contacts:
                if phone != '':
                    cursor_dialer.execute(
                        'INSERT INTO contact '
                        '(id, phone, data, is_original) VALUES (%s, %s, %s, %s)'
                        'ON CONFLICT (id) DO NOTHING;',
                        (id_contact, phone, data, is_original))
                    cursor_dialer.execute(
                        """INSERT INTO contact_in_campaign (id_campaign, id_contact, status,
                    final_status, disposition_option) VALUES (%s, %s, %s, %s, %s);""",
                        (id_campaign, id_contact, STATUS_CREATED, INITIAL,
                         NO_DISPOSITION_OPTION))

    @classmethod
    @job_handler_decorator
    def create_campaign(cls, worker, job):
        # assumes the dialer campaign exists in OML with all the required tables and fields created
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Creating the campaign {id_campaign}')
        contact_strategy = data['contact_strategy']
        prefix = data.get('prefix')

        # Normalizar prefix: convertir lista vacía a None, o lista a string
        if prefix == []:
            prefix = None
        elif isinstance(prefix, list) and len(prefix) > 0:
            prefix = str(prefix[0])
        elif prefix is not None:
            prefix = str(prefix)
        # Si prefix es None, se mantiene como None (aceptado por PostgreSQL)

        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    (
                        campaign_id_data,
                        incidence_rules_data,
                        incidence_rules_disposition_data,
                    ) = cls.get_campaign_data(
                        id_campaign, cursor_oml, contact_strategy)
                    cls.connect_redis_dialer()
                    customdialerdst = campaign_id_data[-1]
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:CUSTOMDIALERDST', customdialerdst)
                    voicebot = cls.REDIS_OML_CONNECTION.hget(
                        f"OML:CAMP:{id_campaign}", "VOICEBOT")
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:VOICEBOT', voicebot or 'False')
                    # campaign_id_data[9] = initial_predictive_model (from queue_table)
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:PREDICTIVE_MODEL',
                        'True' if campaign_id_data[9] else 'False')
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the campaign data into omnidialer')
                    campaign_id_data = campaign_id_data + (prefix,)
                    priority = campaign_id_data[6]
                    cursor_dialer.execute(
                        """INSERT INTO campaign (id, oml_status, name, start_date, end_date,
                        duplicates_control, priority, strategy, wait, initial_predictive_model,
                        initial_boost_factor, max_channels, sunday, monday, tuesday, wednesday,
                        thursday, friday, saturday, hour_start, hour_ends, contact_strategy,
                        dialer_status, metadata, customdialerdst, prefix) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s);""", campaign_id_data)
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules into omnidialer')
                    for incidence_rule in incidence_rules_data:
                        cursor_dialer.execute(
                            """INSERT INTO incidence_rules
                            (id, status, status_custom, max_attempt, retry_later, in_mode,
                            campaign_id) VALUES (%s, %s, %s, %s, %s, %s, %s);""", incidence_rule)
                    logger.debug(
                        f'Campaign {id_campaign}: Inserting the incidence_rules for'
                        ' disposition option into omnidialer')
                    for incidence_rule in incidence_rules_disposition_data:
                        cursor_dialer.execute(
                            """INSERT INTO incidence_rules_disposition
                            (id, disposition_option_id, max_attempt, retry_later, in_mode,
                            campaign_id) VALUES (%s, %s, %s, %s, %s, %s);""", incidence_rule)
                    cls.copy_contacts_from_oml(cursor_dialer, cursor_oml, id_campaign)
                    cls.REDIS_DIALER_CONNECTION.set(f'OML:CALLS:{id_campaign}:DIALER', 0)
                    cls._init_campaign_channel_phases(id_campaign)
                    cls.REDIS_DIALER_CONNECTION.hset(
                        f'CAMP:{id_campaign}:DISTRIBUTION', 'PRIORITY', priority)
                    cls._notify_dialer_event(json.dumps({'type': 'CREATE',
                                    'camp_id': id_campaign}))
                    cls._notify_dialer_event(json.dumps({
                            'type': 'CALLS',
                            'camp_id': id_campaign,
                            'calls': 0,
                        }))

        cls.update_percentages_priority_campaigns()

        response = f'Campaign {id_campaign} with strategy {contact_strategy} created!!!'

        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    def clean_selected_contacts(cls, id_campaign, exclude_contact_ids=None):
        # set contacts marked as SELECTED_CALL back to
        # CREATED status, so they can be consumed by the process campaign
        # this is due to these contacts were marked and not called
        # or at least we didn't receive events from Asterisk to change their state
        logger.debug(f'Campaign {id_campaign}: cleaning broken selected contacts')
        exclude = []
        for raw in (exclude_contact_ids or []):
            try:
                exclude.append(int(raw))
            except (TypeError, ValueError):
                continue
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            if exclude:
                cursor.execute(
                    'UPDATE contact_in_campaign SET status = %s WHERE'
                    ' id_campaign = %s and status = %s'
                    ' AND NOT (id_contact = ANY(%s));',
                    (STATUS_CREATED, id_campaign, STATUS_SELECTED_CALL, exclude))
            else:
                cursor.execute(
                    'UPDATE contact_in_campaign SET status = %s WHERE'
                    ' id_campaign = %s and status = %s;',
                    (STATUS_CREATED, id_campaign, STATUS_SELECTED_CALL))
            row_count = cursor.rowcount
            if row_count > 0:
                logger.debug(
                    f"Campaign {id_campaign}: cleaned broken selected contacts={row_count}")
            return row_count

    @classmethod
    def _in_flight_contact_ids(cls, id_campaign):
        """Contactos con phase key viva (llamada en curso), no el hash CAMP:CHANNELS."""
        if int(id_campaign or 0) == 0:
            return set()
        cls.connect_redis_dialer()
        prefix = f'OML:CALLS:PHASE:{id_campaign}:'
        ids = set()
        try:
            for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                    match=prefix + '*', count=100):
                rest = key[len(prefix):]
                if not rest:
                    continue
                ids.add(rest.split(':', 1)[0])
        except Exception as e:
            logger.debug('Phase key scan failed camp %s: %s', id_campaign, e)
        return ids

    @classmethod
    def _recycle_stuck_selected_if_idle(cls, id_campaign):
        """
        Recupera contactos SELECTED_CALL sin phase key viva.

        take_contacts solo consume STATUS_CREATED. Si un ciclo marcó SELECTED
        y el originate/evento no revirtió el status, el loop queda vivo con
        cupo > 0 y 0 contactos tomables.

        En vuelo real = phase keys OML:CALLS:PHASE:{camp}:*. Ni OML:CALLS
        ni el hash CAMP:CHANNELS bloquean el recycle (pueden quedar zombis
        o desfasados); solo se excluyen contactos con phase key.
        """
        active = cls.get_active_channels(id_campaign)
        phases = cls.get_campaign_channel_phases(id_campaign)
        phase_total = phases.get('TOTAL', 0) or 0
        in_flight = cls._in_flight_contact_ids(id_campaign)
        if in_flight:
            recycled = cls.clean_selected_contacts(
                id_campaign, exclude_contact_ids=in_flight) or 0
            if recycled:
                logger.warning(
                    'Campaign %s: recycle SELECTED except %s in-flight phase '
                    'keys (OML:CALLS=%s hash_oncall=%s recycled=%s)',
                    id_campaign, len(in_flight), active,
                    phases.get(PHASE_ONCALL), recycled,
                )
            return recycled
        # Sin phase keys: grace de reserve reciente (ventana pre-phase).
        if cls._campaign_has_recent_reserve(id_campaign):
            return 0
        if phase_total > 0:
            logger.warning(
                'Campaign %s: ghost CHANNELS hash ringing=%s waiting=%s '
                'oncall=%s with 0 phase keys; reconciling',
                id_campaign,
                phases.get(PHASE_RINGING),
                phases.get(PHASE_WAITING_AGENT),
                phases.get(PHASE_ONCALL),
            )
            cls._init_campaign_channel_phases(id_campaign)
        recycled = cls.clean_selected_contacts(id_campaign) or 0
        if recycled:
            logger.warning(
                'Campaign %s: recycled %s stuck SELECTED_CALL contacts '
                '(no in-flight phase keys, OML:CALLS=%s)',
                id_campaign,
                recycled,
                active,
            )
        return recycled

    @classmethod
    def check_running_job(cls, id_campaign):
        """
        Adquiere lease PROCESS-CAMPAIGN-{id} con token UUID.
        Retorna el token si se adquirió; None si otro worker ya corre.
        """
        token = str(uuid.uuid4())
        acquired = cls.REDIS_DIALER_CONNECTION.set(
            cls._process_campaign_lock_key(id_campaign), token,
            nx=True, ex=PROCESS_CAMPAIGN_LOCK_TTL_SEC)
        if not acquired:
            logger.debug(f'Campaign {id_campaign}: is already running')
            cls._notify_dialer_event(json.dumps({'type': 'ALREADY_RUNNING',
                            'camp_id': id_campaign}))
            return None
        return token

    @classmethod
    @job_handler_decorator
    def start_campaign(cls, worker, job):
        cls.connect_redis_oml()
        if not cls.system_is_active():
            cls._notify_dialer_event(json.dumps({'type': 'SYSTEM_STOPPED',
                            'camp_id': 'all'}))
            return b'Forbidden operation'
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        cls.clean_selected_contacts(id_campaign)
        cls._reset_campaign_pacing_state(id_campaign)
        logger.debug(f'Campaign {id_campaign}: starting the campaign')
        cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=sync_omnileads)
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        return b'Campaign started!'

    @classmethod
    def _reset_campaign_pacing_state(cls, id_campaign):
        """
        Limpia métricas/throttle de pacing al arrancar una campaña.

        Borra METRICS, ATT, THROTTLE_*, PACING. Conserva ART/ACW/AHT/AMD_LATENCY.
        Errores Redis: log + no bloquea el start.
        """
        if int(id_campaign or 0) == 0:
            return
        keys = [
            f'CAMP:{id_campaign}:METRICS',
            f'CAMP:{id_campaign}:ATT',
            f'CAMP:{id_campaign}:THROTTLE_STREAK',  # legacy tick key
            f'CAMP:{id_campaign}:THROTTLE_OVER_SINCE',
            f'CAMP:{id_campaign}:THROTTLE_LATCH',
            f'CAMP:{id_campaign}:PACING',
        ]
        try:
            cls.connect_redis_dialer()
            cls.REDIS_DIALER_CONNECTION.delete(*keys)
            logger.info(
                'Campaign %s: reset pacing state keys=%s', id_campaign, keys,
            )
        except Exception:
            logger.exception(
                'Campaign %s: failed to reset pacing state', id_campaign,
            )

    @classmethod
    def opening_hours_match(cls, cursor, id_campaign):
        cursor.execute('SELECT EXTRACT(DOW FROM CURRENT_DATE) AS day_of_week;')
        day_of_week_int = int(cursor.fetchone()[0])
        day_of_week = WEEK_DAYS[day_of_week_int]
        cursor.execute(f'SELECT {day_of_week} FROM ONLY campaign WHERE id = %s;', (id_campaign,))
        day_of_week_allowed = cursor.fetchone()[0]
        cursor.execute('SELECT * FROM ONLY campaign WHERE id = %s AND CURRENT_TIME BETWEEN'
                       ' hour_start AND hour_ends;', (id_campaign,))
        hour_match = cursor.fetchone()
        if not day_of_week_allowed:
            logger.debug(f'Campaign {id_campaign}: day week not allowed to call')
        elif not hour_match:
            logger.debug(f'Campaign {id_campaign}: in the current time is not allowed to call')
        result = day_of_week_allowed and hour_match
        extra_info = None
        if not result:
            cursor.execute('SELECT EXTRACT(HOUR FROM NOW()) AS current_hour, '
                           'EXTRACT(MINUTE FROM NOW()) AS current_minute;')
            hour, minute = cursor.fetchone()
            hour = int(hour)
            minute = int(minute)
            cursor.execute('SELECT hour_start,hour_ends,'
                           'monday,tuesday,wednesday,thursday,friday,saturday,sunday'
                           ' FROM campaign where id = %s;', (id_campaign,))
            campaign_info = cursor.fetchone()
            cursor.execute('SELECT CURRENT_DATE;')
            current_date = cursor.fetchone()[0]
            extra_info = (day_of_week_allowed, day_of_week_int, hour_match, current_date, hour,
                          minute, campaign_info)
        return result, extra_info

    @classmethod
    def get_campaign_status(cls, id_campaign, dialer_cursor):
        dialer_cursor.execute(
            'SELECT dialer_status FROM ONLY campaign WHERE id = %s', (id_campaign,))
        row = dialer_cursor.fetchone()
        if row is None:
            raise CampaignNotFoundError(
                f'Campaign {id_campaign} does not exist in the dialer database')
        return row[0]

    @classmethod
    def all_contacts_were_attempted(cls, id_campaign):
        """True si no quedan contactos CREATED/SELECTED ni agendas abortadas (fuente: Postgres)."""
        with cls.get_dialer_connection() as conn_dialer:
            cursor = conn_dialer.cursor()
            cursor.execute(
                """SELECT COUNT(*) FROM ONLY contact_in_campaign
                WHERE id_campaign = %s
                AND (status = %s OR status = %s OR schedule_aborted = true);""",
                (id_campaign, STATUS_CREATED, STATUS_SELECTED_CALL),
            )
            row = cursor.fetchone()
            return int((row[0] if row else 0) or 0) == 0

    @classmethod
    def no_active_incidence_rules(cls, id_campaign):
        """True si no hay contactos con final_status PENDING_ATTEMPTS (Postgres)."""
        with cls.get_dialer_connection() as conn_dialer:
            cursor = conn_dialer.cursor()
            cursor.execute(
                """SELECT COUNT(*) FROM ONLY contact_in_campaign
                WHERE id_campaign = %s AND final_status = %s;""",
                (id_campaign, PENDING_ATTEMPTS),
            )
            row = cursor.fetchone()
            return int((row[0] if row else 0) or 0) == 0

    @classmethod
    def _campaign_has_live_calls(cls, id_campaign):
        """
        True si hay llamadas en vuelo: no auto-finalizar ni resetear contadores.

        Phase keys OML:CALLS:PHASE son la señal autoritativa; RESERVE_TS cubre
        el tránsito originate. No usa OML:CALLS/hash (pueden ser ghost y
        bloquearían el finalize para siempre).
        """
        if int(id_campaign or 0) == 0:
            return False
        if cls._in_flight_contact_ids(id_campaign):
            return True
        if cls._campaign_has_recent_reserve(id_campaign):
            return True
        return False

    @classmethod
    def no_active_agendas(cls, id_campaign):
        """
        True  => NO hay agendas activas para la campaña
        False => Sí hay agendas (contador > 0 o se detectan jobs en el scheduler)
        """
        str_id_campaign = str(id_campaign)

        # 0) Chequear contador primero (fast-path, resistente a caídas del jobstore)
        try:
            cls.connect_redis_dialer()
            key = cls._agendas_counter_key(id_campaign)
            cur = cls.REDIS_DIALER_CONNECTION.hget(key, 'AGENDAS')
            if int(cur or 0) > 0:
                return False  # hay agendas, no finalizar
        except Exception as exc:
            logger.warning('Agendas counter unavailable (%s). Falling back to scheduler.', exc)

        # 1) Función auxiliar para matchear jobs por nombre/args/kwargs
        def _job_matches_campaign(job):
            name = getattr(job, 'name', '') or ''
            if str_id_campaign in name:
                return True
            args = getattr(job, 'args', ()) or ()
            kwargs = getattr(job, 'kwargs', {}) or {}
            if any(str(a) == str_id_campaign for a in args):
                return True
            if any(str(v) == str_id_campaign for v in kwargs.values()):
                return True
            return False

        # 2) Reintentos breves por error transitorio del jobstore (Redis)
        attempts = 3
        for i in range(attempts):
            try:
                jobs = SchedulerWorker.SCHEDULER.get_jobs()
                break
            except Exception as exc:
                logger.warning('Scheduler get_jobs() failed (try %d/%d): %s', i + 1, attempts, exc)
                time.sleep(0.1 * (i + 1))
        else:
            # Falló todo: FAIL-SAFE => asumimos que HAY agendas (para no finalizar por glitch)
            logger.error('Scheduler get_jobs() unavailable; fail-safe: assuming agendas exist')
            return False

        # 3) Si encontramos un job matcheando, hay agendas
        for job in jobs:
            if _job_matches_campaign(job):
                return False

        # 4) Ni contador ni scheduler indican agendas: no hay
        return True

    @classmethod
    def campaign_is_active(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status = cls.get_campaign_status(id_campaign, cursor_dialer)
            # notify to OML if the campaign is outdated and pause the campaign
            cursor_dialer.execute(
                """SELECT id
                FROM ONLY campaign
                WHERE end_date >= now()::date
                AND start_date <= now()::date
                AND id = %s;""", (id_campaign,))
            campaign_in_range = cursor_dialer.fetchone()
            if not campaign_in_range:
                logger.debug(f'Campaign {id_campaign}: campaign expired')
                cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=True)
                cls.connect_redis_oml()
                cls._notify_dialer_event(json.dumps({'type': 'EXPIRATION',
                                'camp_id': id_campaign}))
                return False

            # notify to OML if there are no more contacts for call and pause
            # the campaign
            if cls.all_contacts_were_attempted(id_campaign) and \
               cls.no_active_incidence_rules(id_campaign) and \
               cls.no_active_agendas(id_campaign):
                if cls._campaign_has_live_calls(id_campaign):
                    logger.info(
                        'Campaign %s: contacts/agendas drained but live '
                        'calls or recent reserves; defer auto-finalize',
                        id_campaign,
                    )
                    return status == ACTIVE
                logger.debug(f'Campaign {id_campaign}: no more contacts pending for call')
                cls.set_campaign_status(id_campaign, FINALIZED, sync_omnileads=True)
                cls.reset_dialer_calls_counter(
                    id_campaign, reason='auto_finalize_contacts_consumed',
                )
                cls.connect_redis_oml()
                cls._notify_dialer_event(json.dumps({
                        'type': 'CONTACTS_CONSUMED',
                        'camp_id': id_campaign,
                    }))
                return False

            # notify to OML if there are less than
            # PERCENTAGE_PENDING_CALL_THRESHOLD%
            # of contacts pending for call
            # TODO: not sure about the frequency of this notification
            cursor_dialer.execute(
                """SELECT status, count(*) * 100.0 / (
                    SELECT count(*) FROM contact_in_campaign
                    WHERE id_campaign = %s
                )
                FROM ONLY contact_in_campaign
                WHERE status = %s AND id_campaign = %s
                GROUP BY status;""",
                (id_campaign, STATUS_ANSWERED_AGENT, id_campaign)
            )
            percentage_called = cursor_dialer.fetchone()
            percentage_called = percentage_called[1] if percentage_called is not None else 0
            percentage_pending_call = 100 - percentage_called
            if percentage_pending_call <= PERCENTAGE_PENDING_CALL_THRESHOLD:
                logger.debug(f'Campaign {id_campaign}: less than '
                             f'{PERCENTAGE_PENDING_CALL_THRESHOLD}% contacts pending for call')
                cls.connect_redis_oml()
                cls._notify_dialer_event(json.dumps({
                        'type': 'ALMOST_NO_CONTACTS',
                        'camp_id': id_campaign,
                    }))
        return status == ACTIVE

    @classmethod
    def get_number_active_campaigns(cls):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT Count(*) FROM ONLY campaign WHERE dialer_status = %s""",
                (ACTIVE,))
            active_campaigns = cursor.fetchone()[0]
        return active_campaigns

    @classmethod
    def get_agent_ids_campaign(cls, id_campaign):
        with cls.get_oml_connection() as conn_oml:
            cursor_oml = conn_oml.cursor()
            TYPE_DIALER = 2
            STATUS_ACTIVE = 2
            cursor_oml.execute(
                """SELECT ominicontacto_app_agenteprofile.id, COUNT(queue_table.campana_id)
                AS queue__campana__count
                FROM ominicontacto_app_agenteprofile LEFT OUTER JOIN queue_member_table ON
                (ominicontacto_app_agenteprofile.id = queue_member_table.member_id)
                LEFT OUTER JOIN queue_table ON (queue_member_table.queue_name = queue_table.name)
                LEFT OUTER JOIN ominicontacto_app_campana ON
                (queue_table.campana_id = ominicontacto_app_campana.id)
                WHERE ominicontacto_app_agenteprofile.id in
                (
                SELECT ominicontacto_app_agenteprofile.id FROM ominicontacto_app_agenteprofile
                INNER JOIN queue_member_table ON
                (ominicontacto_app_agenteprofile.id = queue_member_table.member_id)
                INNER JOIN queue_table ON (queue_member_table.queue_name = queue_table.name)
                WHERE queue_table.campana_id = %s
                ) AND ominicontacto_app_campana.type = %s AND ominicontacto_app_campana.estado = %s
                GROUP BY ominicontacto_app_agenteprofile.id;""",
                (id_campaign, TYPE_DIALER, STATUS_ACTIVE))
            return dict(cursor_oml.fetchall())

    @classmethod
    def get_campaign_agent_snapshot(cls, id_campaign):
        """
        Snapshot de agentes de la campaña para pacing predictivo.

        Incluye READY (A_free) y busy ONCALL / POSTCALL / PAUSE-ACW
        con elapsed desde OML:AGENT:{id} TIMESTAMP.
        Ponderación multi-cola: weight = 1 / queue_count (igual que READY).
        """
        cls.connect_redis_oml()
        agents_distribution = cls.get_agent_ids_campaign(id_campaign)
        now_ts = int(time.time())

        a_free_raw = 0.0
        total_ready = 0
        a_oncall = 0.0
        a_postcall = 0.0
        a_pause_acw = 0.0
        total_oncall = 0
        total_postcall = 0
        total_pause_acw = 0
        busy_agents = []

        for id_agent, queue_count in agents_distribution.items():
            try:
                queue_count = int(queue_count)
            except (TypeError, ValueError):
                continue
            if queue_count <= 0:
                continue

            weight = 1.0 / float(queue_count)
            key = f'OML:AGENT:{id_agent}'
            status, timestamp = cls.REDIS_OML_CONNECTION.hmget(
                key, 'STATUS', 'TIMESTAMP')
            if not status:
                continue

            if status == 'READY':
                # Wrapup / offer-failure cooldown ACD: no contar como libre.
                try:
                    if cls.REDIS_OML_CONNECTION.exists(
                            f'acd:cooldown:agent:{id_agent}'):
                        continue
                except Exception:
                    pass
                a_free_raw += weight
                total_ready += 1
                continue

            if status not in BUSY_LIBERATION_STATUSES:
                continue

            elapsed_sec = None
            if timestamp is not None and timestamp != '':
                try:
                    elapsed_sec = max(0, now_ts - int(timestamp))
                except (TypeError, ValueError):
                    elapsed_sec = None

            busy_agents.append({
                'agent_id': int(id_agent),
                'status': status,
                'elapsed_sec': elapsed_sec,
                'weight': weight,
            })
            if status == 'ONCALL':
                a_oncall += weight
                total_oncall += 1
            elif status == 'POSTCALL':
                a_postcall += weight
                total_postcall += 1
            else:  # PAUSE-ACW
                a_pause_acw += weight
                total_pause_acw += 1

        if a_free_raw <= 0:
            a_free = 0
        else:
            # Redondear hacia arriba evita perder agentes READY por truncamiento
            # entero cuando se comparten entre múltiples campañas activas.
            a_free = int(ceil(a_free_raw))

        return {
            'a_free': a_free,
            'a_free_raw': a_free_raw,
            'total_ready': total_ready,
            'a_oncall': a_oncall,
            'a_postcall': a_postcall,
            'a_pause_acw': a_pause_acw,
            'a_busy': a_oncall + a_postcall + a_pause_acw,
            'total_oncall': total_oncall,
            'total_postcall': total_postcall,
            'total_pause_acw': total_pause_acw,
            'busy_agents': busy_agents,
        }

    @classmethod
    def _redis_hash_float(cls, key, field, default=0.0):
        cls.connect_redis_dialer()
        raw = cls.REDIS_DIALER_CONNECTION.hget(key, field)
        try:
            return float(raw if raw is not None and raw != '' else default)
        except (TypeError, ValueError):
            return default

    @classmethod
    def get_campaign_att(cls, id_campaign):
        """ATT promedio de CAMP:{id}:ATT (0 si ausente)."""
        if int(id_campaign or 0) == 0:
            return 0.0
        return max(0.0, cls._redis_hash_float(f'CAMP:{id_campaign}:ATT', 'ATT', 0.0))

    @classmethod
    def get_campaign_acw(cls, id_campaign):
        """ACW promedio de CAMP:{id}:ACW (0 si ausente)."""
        if int(id_campaign or 0) == 0:
            return 0.0
        return max(0.0, cls._redis_hash_float(f'CAMP:{id_campaign}:ACW', 'ACW', 0.0))

    @classmethod
    def get_campaign_aht(cls, id_campaign):
        """
        AHT de CAMP:{id}:AHT, o ATT+ACW si el hash AHT aún no existe.
        """
        if int(id_campaign or 0) == 0:
            return 0.0
        cls.connect_redis_dialer()
        raw = cls.REDIS_DIALER_CONNECTION.hget(f'CAMP:{id_campaign}:AHT', 'AHT')
        if raw is not None and raw != '':
            try:
                return max(0.0, float(raw))
            except (TypeError, ValueError):
                pass
        return cls.get_campaign_att(id_campaign) + cls.get_campaign_acw(id_campaign)

    @classmethod
    def get_campaign_art(cls, id_campaign):
        """
        ART promedio de CAMP:{id}:ART.
        None si no hay muestra (ART_COUNT=0 / campo ausente).
        """
        if int(id_campaign or 0) == 0:
            return None
        cls.connect_redis_dialer()
        art_count_raw = cls.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{id_campaign}:ART', 'ART_COUNT')
        try:
            art_count = int(float(art_count_raw or 0))
        except (TypeError, ValueError):
            art_count = 0
        if art_count <= 0:
            return None
        return max(0.0, cls._redis_hash_float(f'CAMP:{id_campaign}:ART', 'ART', 0.0))

    @classmethod
    def campaign_has_amd(cls, id_campaign):
        """True si OML:CAMP:{id}.AMD indica detectar_contestadores activo."""
        cid = int(id_campaign or 0)
        if cid == 0:
            return False
        now = time.time()
        cached = cls._campaign_amd_cache.get(cid)
        if cached and cached[0] > now:
            return bool(cached[1])
        cls.connect_redis_oml()
        try:
            raw = cls.REDIS_OML_CONNECTION.hget(f'OML:CAMP:{cid}', 'AMD')
        except Exception:
            logger.exception('campaign_has_amd: error leyendo OML:CAMP:%s AMD', cid)
            raw = None
        enabled = raw in (True, 'true', '1', 'True', 'yes', 'Yes')
        cls._campaign_amd_cache[cid] = (now + AMD_CONF_CACHE_TTL_SEC, enabled)
        return enabled

    @classmethod
    def get_amd_config_fallback_sec(cls):
        """
        TOTAL_ANALYSIS_TIME de OML:AMD_CONF (ms) → segundos.
        Preferencia OML:AMD_CONF:1; si no hay, SCAN; default DEFAULT_AMD_FALLBACK_SEC.
        """
        now = time.time()
        expires_at, cached_val = cls._amd_conf_fallback_cache
        if cached_val is not None and expires_at > now:
            return float(cached_val)

        cls.connect_redis_oml()
        ms = None
        try:
            raw = cls.REDIS_OML_CONNECTION.hget('OML:AMD_CONF:1', 'TOTAL_ANALYSIS_TIME')
            if raw is not None and raw != '':
                ms = float(raw)
            else:
                for key in cls.REDIS_OML_CONNECTION.scan_iter(
                        match='OML:AMD_CONF:*', count=8):
                    raw = cls.REDIS_OML_CONNECTION.hget(key, 'TOTAL_ANALYSIS_TIME')
                    if raw is not None and raw != '':
                        ms = float(raw)
                        break
        except Exception:
            logger.exception('get_amd_config_fallback_sec: error leyendo OML:AMD_CONF')
            ms = None

        if ms is None or ms < 0:
            sec = float(DEFAULT_AMD_FALLBACK_SEC)
        else:
            sec = float(ms) / 1000.0
        cls._amd_conf_fallback_cache = (now + AMD_CONF_CACHE_TTL_SEC, sec)
        return sec

    @classmethod
    def get_campaign_amd_time(cls, id_campaign):
        """
        Extra AMD para t_ring (H7).
        AMD off → 0; con muestra medida (METRICS.AMD_TIME / AMD_LATENCY) → AVG;
        sin muestra → TOTAL_ANALYSIS_TIME/1000.
        """
        if not cls.campaign_has_amd(id_campaign):
            return 0.0
        cls.connect_redis_dialer()
        count_raw = cls.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{id_campaign}:AMD_LATENCY', 'AMD_COUNT')
        try:
            count = int(float(count_raw or 0))
        except (TypeError, ValueError):
            count = 0
        if count > 0:
            avg = cls._redis_hash_float(
                f'CAMP:{id_campaign}:AMD_LATENCY', 'AMD', 0.0)
            if avg <= 0:
                metrics = cls.get_campaign_metrics(id_campaign) or {}
                try:
                    avg = float(metrics.get('AMD_TIME') or 0.0)
                except (TypeError, ValueError):
                    avg = 0.0
            return max(0.0, avg)
        return max(0.0, cls.get_amd_config_fallback_sec())

    @classmethod
    def get_campaign_t_ring(cls, id_campaign):
        """
        Horizonte de predicción t_ring = ART + amd_extra (H7).
        Sin muestra ART usa DEFAULT_ART_SEC.
        amd_extra: 0 si AMD off; media medida; o fallback TOTAL_ANALYSIS_TIME.
        """
        art = cls.get_campaign_art(id_campaign)
        if art is None:
            art = float(DEFAULT_ART_SEC)
        amd_time = cls.get_campaign_amd_time(id_campaign)
        return max(0.0, art) + max(0.0, amd_time)

    @staticmethod
    def estimate_remaining_busy_sec(status, elapsed_sec, aht, att, acw):
        """
        Segundos estimados hasta READY según estado ocupado.
        ONCALL: ciclo AHT (ATT+ACW) desde TIMESTAMP de entrada a llamada.
        POSTCALL / PAUSE-ACW: media ACW.
        None si no hay media usable para ese estado.
        """
        try:
            elapsed = float(elapsed_sec) if elapsed_sec is not None else 0.0
        except (TypeError, ValueError):
            elapsed = 0.0
        elapsed = max(0.0, elapsed)
        try:
            aht = max(0.0, float(aht or 0.0))
        except (TypeError, ValueError):
            aht = 0.0
        try:
            att = max(0.0, float(att or 0.0))
        except (TypeError, ValueError):
            att = 0.0
        try:
            acw = max(0.0, float(acw or 0.0))
        except (TypeError, ValueError):
            acw = 0.0

        if status == 'ONCALL':
            mean_total = aht if aht > 0 else (att + acw)
            if mean_total <= 0:
                return None
            return max(float(P_LIB_REMAINING_EPS), mean_total - elapsed)
        if status in ('POSTCALL', 'PAUSE-ACW'):
            mean_acw = acw
            if mean_acw <= 0 and aht > att > 0:
                mean_acw = aht - att
            if mean_acw <= 0:
                return None
            return max(float(P_LIB_REMAINING_EPS), mean_acw - elapsed)
        return None

    @classmethod
    def compute_agent_p_lib(cls, status, elapsed_sec, horizon_sec, aht, att, acw):
        """
        P_lib = 1 - exp(-t_ring / remaining)  (exponencial de supervivencia).
        0 si el agente no es busy liberable o faltan medias.
        """
        if status not in BUSY_LIBERATION_STATUSES:
            return 0.0
        try:
            horizon = max(0.0, float(horizon_sec or 0.0))
        except (TypeError, ValueError):
            horizon = 0.0
        if horizon <= 0:
            return 0.0
        remaining = cls.estimate_remaining_busy_sec(status, elapsed_sec, aht, att, acw)
        if remaining is None or remaining <= 0:
            return 0.0
        return 1.0 - exp(-horizon / remaining)

    @classmethod
    def compute_a_expected(cls, busy_agents, horizon_sec, aht, att, acw):
        """
        A_expected = sum_i (P_lib,i * weight_i) sobre busy liberables.
        Devuelve (a_expected, details) con p_lib/remaining por agente.
        """
        total = 0.0
        details = []
        for item in busy_agents or []:
            status = item.get('status')
            weight = item.get('weight', 1.0)
            try:
                weight = float(weight)
            except (TypeError, ValueError):
                weight = 0.0
            elapsed = item.get('elapsed_sec')
            remaining = cls.estimate_remaining_busy_sec(
                status, elapsed, aht, att, acw,
            )
            p_lib = cls.compute_agent_p_lib(
                status, elapsed, horizon_sec, aht, att, acw,
            )
            contrib = p_lib * weight
            total += contrib
            details.append({
                'agent_id': item.get('agent_id'),
                'status': status,
                'elapsed_sec': elapsed,
                'weight': weight,
                'remaining_sec': remaining,
                'p_lib': p_lib,
                'contribution': contrib,
            })
        return total, details

    @classmethod
    def get_campaign_a_expected(cls, id_campaign, horizon_sec=None, snapshot=None):
        """
        Calcula A_expected(t_ring) para la campaña (H4).
        No modifica el pacing; H5 usará este valor en C_dial.
        snapshot opcional evita un segundo scan de OML:AGENT:*.
        """
        if int(id_campaign or 0) == 0:
            return {
                'a_expected': 0.0,
                't_ring': 0.0,
                'aht': 0.0,
                'att': 0.0,
                'acw': 0.0,
                'details': [],
            }
        if snapshot is None:
            snapshot = cls.get_campaign_agent_snapshot(id_campaign)
        att = cls.get_campaign_att(id_campaign)
        acw = cls.get_campaign_acw(id_campaign)
        aht = cls.get_campaign_aht(id_campaign)
        if horizon_sec is None:
            t_ring = cls.get_campaign_t_ring(id_campaign)
        else:
            try:
                t_ring = max(0.0, float(horizon_sec))
            except (TypeError, ValueError):
                t_ring = cls.get_campaign_t_ring(id_campaign)
        a_expected, details = cls.compute_a_expected(
            snapshot.get('busy_agents') or [],
            t_ring,
            aht=aht,
            att=att,
            acw=acw,
        )
        return {
            'a_expected': a_expected,
            't_ring': t_ring,
            'aht': aht,
            'att': att,
            'acw': acw,
            'details': details,
        }

    @classmethod
    def get_number_available_agents(cls, id_campaign):
        snapshot = cls.get_campaign_agent_snapshot(id_campaign)
        return snapshot['a_free'], snapshot['total_ready']

    @classmethod
    def get_active_channels(cls, id_campaign: int) -> int:
        cls.connect_redis_dialer()
        key = f'OML:CALLS:{id_campaign}:DIALER'

        val = cls.REDIS_DIALER_CONNECTION.get(key)
        if val is None:
            created = cls.REDIS_DIALER_CONNECTION.set(key, 0, nx=True)
            logger.debug(
                'OML:CALLS:%s:DIALER missing; init=%s',
                id_campaign, bool(created)
            )
            if created:
                try:
                    cls.connect_redis_oml()
                    cls._notify_dialer_event(json.dumps(
                            {'type': 'CALLS', 'camp_id':
                             id_campaign, 'calls': 0})
                    )
                except Exception as e:
                    logger.debug('Publish init CALLS failed: %s', e)
            return 0

        try:
            n = int(val)
        except (TypeError, ValueError):
            logger.warning(
                'Invalid CALLS "%s" camp %s; resetting to 0', val, id_campaign
            )
            cls.REDIS_DIALER_CONNECTION.set(key, 0)
            return 0
        if n < 0:
            logger.warning(
                'Negative CALLS %s for camp %s; fixing to 0', n, id_campaign
            )
            cls.REDIS_DIALER_CONNECTION.set(key, 0)
            return 0

        return n

    @classmethod
    def _channels_hash_key(cls, id_campaign):
        return f'CAMP:{id_campaign}:CHANNELS'

    @classmethod
    def _phase_contact_key(cls, id_campaign, contact_id):
        return f'OML:CALLS:PHASE:{id_campaign}:{contact_id}'

    @classmethod
    def _phase_callid_key(cls, id_campaign, contact_id, callid):
        if callid:
            return f'OML:CALLS:PHASE:{id_campaign}:{contact_id}:{callid}'
        return cls._phase_contact_key(id_campaign, contact_id)

    @classmethod
    def _clear_campaign_channel_phases(cls, id_campaign):
        """Borra hash de fases y phase keys de una campaña."""
        if int(id_campaign or 0) == 0:
            return
        cls.connect_redis_dialer()
        cls.REDIS_DIALER_CONNECTION.delete(cls._channels_hash_key(id_campaign))
        pattern = f'OML:CALLS:PHASE:{id_campaign}:*'
        try:
            for key in cls.REDIS_DIALER_CONNECTION.scan_iter(match=pattern, count=100):
                cls.REDIS_DIALER_CONNECTION.delete(key)
        except Exception as e:
            logger.debug(
                'Phase key cleanup failed camp %s: %s', id_campaign, e,
            )

    @classmethod
    def _align_phases_after_audit_down(cls, id_campaign, safe_down_target):
        """
        Tras bajar OML:CALLS por audit, alinea PHASE keys.

        target==0 → purge total (CALLS forzado a 0 con cluster completo).
        target>0 → solo puede borrar candidatos RINGING viejos (sin RESERVE_TS
        reciente). Nunca WAITING_AGENT/ONCALL: una llamada con agente no debe
        perder la phase key por un scan arbitrario.
        """
        if int(id_campaign or 0) == 0:
            return
        cls.connect_redis_dialer()
        r = cls.REDIS_DIALER_CONNECTION
        target = max(0, int(safe_down_target or 0))
        if target == 0:
            cls._clear_campaign_channel_phases(id_campaign)
            cls._init_campaign_channel_phases(id_campaign)
            try:
                for key in r.scan_iter(
                    match=f'OML:CALLS:RESERVE_TS:{id_campaign}:*', count=200,
                ):
                    r.delete(key)
            except Exception:
                logger.debug(
                    'RESERVE_TS cleanup failed camp %s', id_campaign, exc_info=True,
                )
            return

        phase_pattern = f'OML:CALLS:PHASE:{id_campaign}:*'
        prefix = f'OML:CALLS:PHASE:{id_campaign}:'
        try:
            keys = list(r.scan_iter(match=phase_pattern, count=200))
        except Exception:
            logger.debug(
                'Phase scan failed camp %s', id_campaign, exc_info=True,
            )
            return
        if len(keys) <= target:
            return

        now = int(time.time())
        recent_grace = min(RESERVE_GRACE_SEC, 8)
        # (age_sec, key) — age alto / sin TS primero; solo RINGING elegibles.
        purge_candidates = []
        protected = 0
        for key in keys:
            phase_val = cls._decode_redis_str(r.get(key)) or ''
            if phase_val in (PHASE_WAITING_AGENT, PHASE_ONCALL):
                protected += 1
                continue
            if phase_val != PHASE_RINGING:
                # Valor desconocido: no tocar (fail-safe).
                protected += 1
                continue
            key_s = cls._decode_redis_str(key) or ''
            rest = key_s[len(prefix):] if key_s.startswith(prefix) else ''
            contact_id = rest.split(':', 1)[0] if rest else ''
            age = None
            if contact_id:
                rts = r.get(f'OML:CALLS:RESERVE_TS:{id_campaign}:{contact_id}')
                if rts is not None:
                    try:
                        age = now - int(rts)
                    except (TypeError, ValueError):
                        age = None
            if age is not None and age < recent_grace:
                protected += 1
                continue
            # Sin TS o TS viejo: candidato. age None → más viejo que cualquier TS.
            purge_candidates.append(
                (10**12 if age is None else age, key),
            )

        excess = len(keys) - target
        if excess <= 0 or not purge_candidates:
            if excess > 0:
                logger.info(
                    'Audit align camp %s: excess=%s but no RINGING purge '
                    'candidates (protected=%s); skip',
                    id_campaign, excess, protected,
                )
            return

        purge_candidates.sort(key=lambda item: item[0], reverse=True)
        to_delete = purge_candidates[:excess]
        deleted = 0
        for _age, key in to_delete:
            r.delete(key)
            deleted += 1
        if deleted:
            logger.warning(
                'Audit aligned phase keys camp %s: purged_ringing=%s '
                'protected=%s remaining_est=%s target=%s',
                id_campaign, deleted, protected, len(keys) - deleted, target,
            )

    @classmethod
    def _init_campaign_channel_phases(cls, id_campaign):
        """Inicializa contadores de fase en 0 (create / reset)."""
        if int(id_campaign or 0) == 0:
            return
        cls.connect_redis_dialer()
        cls.REDIS_DIALER_CONNECTION.hset(
            cls._channels_hash_key(id_campaign),
            mapping={
                PHASE_RINGING: 0,
                PHASE_WAITING_AGENT: 0,
                PHASE_ONCALL: 0,
            },
        )

    @classmethod
    def _empty_channel_phases(cls):
        return {
            PHASE_RINGING: 0,
            PHASE_WAITING_AGENT: 0,
            PHASE_ONCALL: 0,
            'TOTAL': 0,
        }

    @classmethod
    def _phases_from_raw_hash(cls, raw):
        raw = raw or {}

        def _int(name):
            try:
                return max(0, int(raw.get(name, 0) or 0))
            except (TypeError, ValueError):
                return 0

        ringing = _int(PHASE_RINGING)
        waiting = _int(PHASE_WAITING_AGENT)
        oncall = _int(PHASE_ONCALL)
        return {
            PHASE_RINGING: ringing,
            PHASE_WAITING_AGENT: waiting,
            PHASE_ONCALL: oncall,
            'TOTAL': ringing + waiting + oncall,
        }

    @classmethod
    def get_campaign_channel_phases(cls, id_campaign):
        """
        Lee CAMP:{id}:CHANNELS. Devuelve {RINGING, WAITING_AGENT, ONCALL, TOTAL}.
        """
        if int(id_campaign or 0) == 0:
            return cls._empty_channel_phases()
        cls.connect_redis_dialer()
        raw = cls.REDIS_DIALER_CONNECTION.hgetall(cls._channels_hash_key(id_campaign)) or {}
        return cls._phases_from_raw_hash(raw)

    @classmethod
    def _get_calls_and_phases(cls, id_campaign):
        """
        Snapshot atómico (pipeline) de OML:CALLS + CAMP:{id}:CHANNELS.
        Evita TOCTOU entre total y desglose de fases en el mismo tick de pacing.
        """
        if int(id_campaign or 0) == 0:
            return 0, cls._empty_channel_phases()
        cls.connect_redis_dialer()
        key_calls = f'OML:CALLS:{id_campaign}:DIALER'
        try:
            pipe = cls.REDIS_DIALER_CONNECTION.pipeline()
            pipe.get(key_calls)
            pipe.hgetall(cls._channels_hash_key(id_campaign))
            results = pipe.execute()
            if not results or len(results) < 2:
                raise RuntimeError('pipeline returned incomplete results')
            val, raw = results[0], results[1]
        except Exception:
            logger.debug(
                'Campaign %s: pipeline CALLS+CHANNELS failed; fallback',
                id_campaign, exc_info=True,
            )
            return cls.get_active_channels(id_campaign), cls.get_campaign_channel_phases(
                id_campaign,
            )
        try:
            active = max(0, int(val or 0))
        except (TypeError, ValueError):
            active = 0
        return active, cls._phases_from_raw_hash(raw)

    @classmethod
    def _reserve_dialer_channel(cls, id_campaign, contact_id) -> str:
        """
        Reserva un cupo en OML:CALLS:{camp}:DIALER (INCR + check max_channels)
        y marca fase RINGING en CAMP:{camp}:CHANNELS.

        Retorna RESERVE_OK | RESERVE_PHASE_EXISTS | RESERVE_MAX_CHANNELS |
        RESERVE_ERROR. Con phase existente no se reencola a CREATED.
        """
        if int(id_campaign or 0) == 0:
            return RESERVE_ERROR
        cls.connect_redis_dialer()
        key_calls = f'OML:CALLS:{id_campaign}:DIALER'
        channels_key = cls._channels_hash_key(id_campaign)
        phase_key = cls._phase_contact_key(id_campaign, contact_id)
        reserve_ts_key = f'OML:CALLS:RESERVE_TS:{id_campaign}:{contact_id}'
        campaign_max = cls.get_campaign_max_available_channels(id_campaign)
        reserve_ttl = max(RESERVE_GRACE_SEC * 4, 120)
        try:
            result = cls.REDIS_DIALER_CONNECTION.eval(
                _RESERVE_CHANNEL_LUA, 4,
                key_calls, channels_key, phase_key, reserve_ts_key,
                int(campaign_max), PHASE_RINGING,
                str(int(time.time())), reserve_ttl, CALLS_PHASE_TTL_SEC,
            )
        except Exception:
            logger.exception(
                'Campaign %s: reserve channel Lua failed contact=%s',
                id_campaign, contact_id,
            )
            return RESERVE_ERROR
        ok = int(result[0]) == 1 if result else False
        current = int(result[1]) if result and len(result) > 1 else 0
        reason = result[2] if result and len(result) > 2 else ''
        if isinstance(reason, bytes):
            reason = reason.decode('utf-8', errors='replace')
        if not ok:
            cls._publish_calls_count(id_campaign)
            if reason == RESERVE_PHASE_EXISTS:
                existing_phase = cls.REDIS_DIALER_CONNECTION.get(phase_key)
                logger.debug(
                    'Campaign %s: reserve skipped, contact %s already in phase %s '
                    '(calls=%s)',
                    id_campaign, contact_id, existing_phase, current,
                )
                return RESERVE_PHASE_EXISTS
            logger.warning(
                'Campaign %s: Exceeded max channels (%s > %s). Skipping contact %s',
                id_campaign, current + 1, campaign_max, contact_id,
            )
            return RESERVE_MAX_CHANNELS if reason == RESERVE_MAX_CHANNELS else RESERVE_ERROR
        # Nuevo intento: limpiar marker de rollback del ciclo anterior.
        try:
            cls.REDIS_DIALER_CONNECTION.delete(
                f'OML:CALLS:DECR:{id_campaign}:{contact_id}',
            )
        except Exception:
            pass
        # M0: attempt_id dialer-owned (aditivo; no cambia cupo).
        if ATTEMPT_ID_ENABLED:
            try:
                store_attempt_id(
                    cls.REDIS_DIALER_CONNECTION,
                    id_campaign,
                    contact_id,
                    new_attempt_id(),
                    CALLS_PHASE_TTL_SEC,
                )
            except Exception:
                logger.debug(
                    'Campaign %s: store attempt_id failed contact=%s',
                    id_campaign, contact_id, exc_info=True,
                )
        cls._publish_calls_count(id_campaign)
        return RESERVE_OK

    @classmethod
    def _transition_channel_phase(cls, id_campaign, contact_id, callid, target_phase):
        """
        Avanza la fase de un canal (forward-only). Sin phase viva → no-op
        (no adopta buckets fantasma).
        """
        if int(id_campaign or 0) == 0:
            return False
        if target_phase not in CHANNEL_PHASE_RANK:
            logger.warning(
                'Invalid channel phase transition camp=%s target=%s',
                id_campaign, target_phase,
            )
            return False
        cls.connect_redis_dialer()
        channels_key = cls._channels_hash_key(id_campaign)
        phase_contact = cls._phase_contact_key(id_campaign, contact_id)
        phase_callid = cls._phase_callid_key(id_campaign, contact_id, callid)
        try:
            result = cls.REDIS_DIALER_CONNECTION.eval(
                _PHASE_TRANSITION_LUA, 3,
                channels_key, phase_contact, phase_callid,
                target_phase, CALLS_PHASE_TTL_SEC,
            )
        except Exception:
            logger.exception(
                'Channel phase transition failed camp=%s contact=%s callid=%s target=%s',
                id_campaign, contact_id, callid, target_phase,
            )
            return False
        changed = int(result[0]) == 1 if result else False
        reason = result[1] if result and len(result) > 1 else ''
        if isinstance(reason, bytes):
            reason = reason.decode('utf-8', errors='replace')
        logger.debug(
            'Channel phase camp=%s contact=%s callid=%s target=%s changed=%s reason=%s',
            id_campaign, contact_id, callid, target_phase, changed, reason,
        )
        return changed

    @classmethod
    def _mark_contact_status_created(cls, id_campaign, contact_id):
        with cls.get_dialer_connection() as conn:
            conn.cursor().execute(
                'UPDATE contact_in_campaign SET status = %s '
                'WHERE id_contact = %s AND id_campaign = %s',
                (STATUS_CREATED, contact_id, id_campaign),
            )

    @classmethod
    def _publish_calls_count(cls, id_campaign: int) -> None:
        """
        Publica el valor actual de OML:CALLS:{id_campaign}:DIALER en OML:STREAM:DIALER
        para que la supervisión actualice "Canales discando" en tiempo real.
        """
        try:
            cls.connect_redis_dialer()
            key = f'OML:CALLS:{id_campaign}:DIALER'
            val = cls.REDIS_DIALER_CONNECTION.get(key)
            if val is None:
                count = 0
            else:
                try:
                    count = int(val)
                except (TypeError, ValueError):
                    count = 0
            cls.connect_redis_oml()
            cls._notify_dialer_event(json.dumps({'type': 'CALLS', 'camp_id': id_campaign, 'calls': count})
            )
        except Exception as e:
            logger.debug('Publish CALLS count failed camp %s: %s', id_campaign, e)

    @classmethod
    def _calls_decr_dedup_key(cls, id_campaign, contact_id, callid):
        """Dedup por callid si existe; si no, clave estable por contacto (rollbacks)."""
        if callid:
            return f'OML:CALLS:DECR:{id_campaign}:{contact_id}:{callid}'
        return f'OML:CALLS:DECR:{id_campaign}:{contact_id}'

    @classmethod
    def _refresh_campaign_aht(cls, id_campaign):
        """
        Deriva AHT = ATT + ACW (promedios de campaña) en Redis DB3.
        Hash CAMP:{id}:AHT → AHT.
        Ausencia de ATT o ACW se trata como 0.
        """
        if int(id_campaign or 0) == 0:
            return
        cls.connect_redis_dialer()
        try:
            att_raw = cls.REDIS_DIALER_CONNECTION.hget(f'CAMP:{id_campaign}:ATT', 'ATT')
            acw_raw = cls.REDIS_DIALER_CONNECTION.hget(f'CAMP:{id_campaign}:ACW', 'ACW')
            try:
                att = float(att_raw or 0.0)
            except (TypeError, ValueError):
                att = 0.0
            try:
                acw = float(acw_raw or 0.0)
            except (TypeError, ValueError):
                acw = 0.0
            aht = att + acw
            cls.REDIS_DIALER_CONNECTION.hset(f'CAMP:{id_campaign}:AHT', 'AHT', aht)
            logger.debug(
                'Campaign %s: AHT refreshed att=%s acw=%s aht=%s',
                id_campaign, att, acw, aht,
            )
        except Exception:
            logger.exception(
                'Error refreshing campaign AHT camp=%s',
                id_campaign,
            )

    @classmethod
    def _update_campaign_att(cls, id_campaign, agent_duration):
        """
        Actualiza el ATT (Average Talk Time) de campaña en Redis DB3.
        Hash CAMP:{id}:ATT → ATT_SUM, ATT_COUNT, ATT (promedio aritmético).
        También refresca AHT = ATT + ACW.
        """
        if int(id_campaign or 0) == 0:
            return
        try:
            duration = max(0.0, float(agent_duration))
        except (TypeError, ValueError):
            duration = 0.0
        cls.connect_redis_dialer()
        key = f'CAMP:{id_campaign}:ATT'
        try:
            cls.REDIS_DIALER_CONNECTION.eval(
                _UPDATE_CAMPAIGN_ATT_LUA, 1, key, duration,
            )
        except Exception:
            logger.exception(
                'Error updating campaign ATT camp=%s duration=%s',
                id_campaign, duration,
            )
            return
        cls._refresh_campaign_aht(id_campaign)

    @classmethod
    def _incr_sin_disposicion_if_unqualified(cls, id_campaign, contact_id):
        """
        EXIT_ANSWERED sin calificación: CAMP:{id}:COUNTER SIN_DISPOSICION += 1.
        """
        if int(id_campaign or 0) == 0 or int(contact_id or 0) == 0:
            return
        disp = NO_DISPOSITION_OPTION
        try:
            with cls.get_dialer_connection() as conn_dialer:
                cursor = conn_dialer.cursor()
                cursor.execute(
                    'SELECT disposition_option FROM ONLY contact_in_campaign '
                    'WHERE id_campaign = %s AND id_contact = %s;',
                    (id_campaign, contact_id),
                )
                row = cursor.fetchone()
            if row and row[0] is not None:
                disp = int(row[0])
        except Exception:
            logger.exception(
                'Error leyendo disposition_option camp=%s contact=%s',
                id_campaign, contact_id,
            )
            return
        if disp != NO_DISPOSITION_OPTION:
            return
        cls.connect_redis_dialer()
        try:
            cls.REDIS_DIALER_CONNECTION.hincrby(
                f'CAMP:{id_campaign}:COUNTER',
                SIN_DISPOSICION_COUNTER_KEY,
            )
        except Exception:
            logger.exception(
                'Error incrementando SIN_DISPOSICION camp=%s',
                id_campaign,
            )

    @classmethod
    def _update_campaign_art(cls, id_campaign, ring_duration):
        """
        Actualiza el ART (Average Ring Time) de campaña en Redis DB3.
        Hash CAMP:{id}:ART → ART_SUM, ART_COUNT, ART (promedio aritmético).
        ring_duration: segundos originate PSTN → Dial ANSWER to_pstn.
        """
        if int(id_campaign or 0) == 0:
            return
        try:
            duration = max(0.0, float(ring_duration))
        except (TypeError, ValueError):
            return
        cls.connect_redis_dialer()
        key = f'CAMP:{id_campaign}:ART'
        try:
            cls.REDIS_DIALER_CONNECTION.eval(
                _UPDATE_CAMPAIGN_ART_LUA, 1, key, duration,
            )
        except Exception:
            logger.exception(
                'Error updating campaign ART camp=%s duration=%s',
                id_campaign, duration,
            )

    @classmethod
    def _update_campaign_amd_latency(cls, id_campaign, amd_duration):
        """
        Actualiza latencia AMD media (H7) en Redis DB3.
        Hash CAMP:{id}:AMD_LATENCY → AMD_SUM, AMD_COUNT, AMD;
        también METRICS.AMD_TIME = AVG.
        """
        if int(id_campaign or 0) == 0:
            return
        try:
            duration = max(0.0, float(amd_duration))
        except (TypeError, ValueError):
            return
        cls.connect_redis_dialer()
        lat_key = f'CAMP:{id_campaign}:AMD_LATENCY'
        metrics_key = f'CAMP:{id_campaign}:METRICS'
        try:
            cls.REDIS_DIALER_CONNECTION.eval(
                _UPDATE_CAMPAIGN_AMD_LATENCY_LUA, 2, lat_key, metrics_key, duration,
            )
        except Exception:
            logger.exception(
                'Error updating campaign AMD latency camp=%s duration=%s',
                id_campaign, duration,
            )

    @classmethod
    def _update_campaign_acw(cls, id_campaign, acw_duration):
        """
        Actualiza el ACW (Average After Call Work) de campaña en Redis DB3.
        Hash CAMP:{id}:ACW → ACW_SUM, ACW_COUNT, ACW (promedio aritmético).
        acw_duration: segundos en PAUSE-ACW (tipificación) atribuidos a la campaña.
        También refresca AHT = ATT + ACW.
        """
        if int(id_campaign or 0) == 0:
            return
        try:
            duration = max(0.0, float(acw_duration))
        except (TypeError, ValueError):
            return
        cls.connect_redis_dialer()
        key = f'CAMP:{id_campaign}:ACW'
        try:
            cls.REDIS_DIALER_CONNECTION.eval(
                _UPDATE_CAMPAIGN_ACW_LUA, 1, key, duration,
            )
        except Exception:
            logger.exception(
                'Error updating campaign ACW camp=%s duration=%s',
                id_campaign, duration,
            )
            return
        cls._refresh_campaign_aht(id_campaign)

    @classmethod
    def update_campaign_hit(cls, id_campaign, hit, abandon=False, tech=False):
        """
        Actualiza P_HIT / DROP_RATE y contadores en CAMP:{id}:METRICS (Redis dialer DB3).

        hit=True  → HIT (ANSWERED_PSTN); CONNECT_COUNT == HIT_COUNT
        hit=False, abandon=True  → ABANDON (EXIT_ABANDON / EXIT_TIMEOUT); no toca P_HIT
        tech=True → TECH_FAIL_COUNT++; no toca P_HIT / FAIL_COUNT
        hit=False, abandon=False, tech=False → FAIL (BUSY, NOANSWER, AMD, …)
        """
        if int(id_campaign or 0) == 0:
            return
        hit_flag = 1 if hit else 0
        abandon_flag = 1 if abandon and not hit else 0
        tech_flag = 1 if tech and not hit and not abandon else 0
        cls.connect_redis_dialer()
        key = f'CAMP:{id_campaign}:METRICS'
        try:
            cls.REDIS_DIALER_CONNECTION.eval(
                _UPDATE_CAMPAIGN_HIT_LUA, 1, key,
                hit_flag, abandon_flag, P_HIT_ALPHA, DROP_RATE_ALPHA, tech_flag,
            )
        except Exception:
            logger.exception(
                'Error updating campaign hit metrics camp=%s hit=%s abandon=%s tech=%s',
                id_campaign, hit, abandon, tech,
            )

    @classmethod
    def get_campaign_metrics(cls, id_campaign):
        """
        Lee CAMP:{id}:METRICS. Devuelve dict tipado o None si campaña 0 / sin hash.
        """
        if int(id_campaign or 0) == 0:
            return None
        cls.connect_redis_dialer()
        raw = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:METRICS')
        if not raw:
            return None

        def _float(name, default=0.0):
            try:
                return float(raw.get(name, default) or default)
            except (TypeError, ValueError):
                return default

        def _int(name, default=0):
            try:
                return int(float(raw.get(name, default) or default))
            except (TypeError, ValueError):
                return default

        hit_count = _int('HIT_COUNT')
        fail_count = _int('FAIL_COUNT')
        abandon_count = _int('ABANDON_COUNT')
        connect_count = _int('CONNECT_COUNT')
        # CONNECT_COUNT canónico == HIT_COUNT (connects humanos). Si datos legacy
        # inflaron CONNECT, preferir HIT_COUNT cuando existe.
        if hit_count > 0:
            connect_count = hit_count
        drop_rate = _float('DROP_RATE')
        if hit_count > 0 and 'DROP_RATE' not in raw:
            drop_rate = abandon_count / float(hit_count)
        p_hit_ratio = _float('P_HIT_RATIO')
        if (hit_count + fail_count) > 0 and 'P_HIT_RATIO' not in raw:
            p_hit_ratio = hit_count / float(hit_count + fail_count)

        return {
            'P_HIT': _float('P_HIT'),
            'P_HIT_RATIO': p_hit_ratio,
            'HIT_COUNT': hit_count,
            'FAIL_COUNT': fail_count,
            'TECH_FAIL_COUNT': _int('TECH_FAIL_COUNT'),
            'ABANDON_COUNT': abandon_count,
            'CONNECT_COUNT': connect_count,
            'DROP_RATE': drop_rate,
            'DROP_RATE_EWMA': _float('DROP_RATE_EWMA'),
            'HAS_DROP_RATE_EWMA': 'DROP_RATE_EWMA' in raw,
            'AMD_TIME': _float('AMD_TIME', AMD_TIME),
            'WINDOW_MODE': str(raw.get('WINDOW_MODE') or 'ewma'),
        }

    @classmethod
    def _decrement_calls_once(cls, id_campaign, contact_id, callid, context='', use_dedup=True):
        """
        Decrementa OML:CALLS y el bucket de fase persistido.
        Con dedup evita doble DECR por eventos duplicados.
        """
        if int(id_campaign or 0) == 0:
            return False
        cls.connect_redis_dialer()
        # Sin callid (rollback): siempre dedup estable por contacto para que un
        # ChannelDestroyed/Dial fail posterior no haga doble DECR.
        if not callid:
            use_dedup = True
        contact_marker = f'OML:CALLS:DECR:{id_campaign}:{contact_id}'
        # Evento con callid tras rollback (u otro finalize del contacto): skip.
        if use_dedup and callid:
            try:
                if cls.REDIS_DIALER_CONNECTION.exists(contact_marker):
                    logger.debug(
                        'Skip decrement camp=%s contact=%s callid=%s ctx=%s '
                        '(contact already finalized)',
                        id_campaign, contact_id, callid, context,
                    )
                    return False
            except Exception:
                pass
        dedup_key = cls._calls_decr_dedup_key(id_campaign, contact_id, callid)
        key_calls = f'OML:CALLS:{id_campaign}:DIALER'
        channels_key = cls._channels_hash_key(id_campaign)
        phase_contact = cls._phase_contact_key(id_campaign, contact_id)
        phase_callid = cls._phase_callid_key(id_campaign, contact_id, callid)
        try:
            result = cls.REDIS_DIALER_CONNECTION.eval(
                _FINALIZE_CHANNEL_LUA, 5,
                key_calls, channels_key, phase_contact, phase_callid, dedup_key,
                1 if use_dedup else 0, CALLS_DECR_DEDUP_TTL_SEC,
            )
        except Exception as e:
            logger.error(
                'Campaign %s: error decrementing call count on %s: %s',
                id_campaign, context, e,
                exc_info=True,
            )
            return False
        if not result:
            return False
        ok = int(result[0]) == 1
        if not ok:
            reason = result[1] if len(result) > 1 else ''
            if isinstance(reason, bytes):
                reason = reason.decode('utf-8', errors='replace')
            if reason == 'dup':
                logger.debug(
                    'Skip duplicate decrement camp=%s contact=%s callid=%s ctx=%s',
                    id_campaign, contact_id, callid, context,
                )
            return False
        orphan_flag = result[1] if len(result) > 1 else '0'
        if isinstance(orphan_flag, bytes):
            orphan_flag = orphan_flag.decode('utf-8', errors='replace')
        if orphan_flag == '1':
            logger.warning(
                'Campaign %s: finalize without phase key (orphan) contact=%s '
                'callid=%s ctx=%s',
                id_campaign, contact_id, callid, context,
            )
        try:
            total_after = int(result[2]) if len(result) > 2 else None
        except (TypeError, ValueError):
            total_after = None
        if total_after is not None and total_after == 0:
            # posiblemente reset desde negativo ya cubierto en Lua
            pass
        # Marker contacto (TTL corto): cruza Dial terminal con callid ↔
        # ChannelDestroyed sin callid (o callid distinto del mismo hangup).
        # Reserve borra el marker al reintentar el contacto.
        if use_dedup:
            try:
                cls.REDIS_DIALER_CONNECTION.set(
                    contact_marker, '1', nx=True,
                    ex=CALLS_CONTACT_DECR_MARKER_TTL_SEC,
                )
            except Exception:
                pass
        try:
            cls.REDIS_DIALER_CONNECTION.delete(
                f'OML:CALLS:RESERVE_TS:{id_campaign}:{contact_id}',
            )
        except Exception:
            pass
        # Defensa: barrer residuales del contacto (race transition-noop SET
        # tras DEL de finalize, o phase_callid distinto al pasado).
        residual = 0
        try:
            contact_key = cls._phase_contact_key(id_campaign, contact_id)
            if cls.REDIS_DIALER_CONNECTION.delete(contact_key):
                residual += 1
            for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                    match=f'OML:CALLS:PHASE:{id_campaign}:{contact_id}:*',
                    count=20):
                cls.REDIS_DIALER_CONNECTION.delete(key)
                residual += 1
        except Exception:
            logger.debug(
                'Phase residual sweep failed camp=%s contact=%s',
                id_campaign, contact_id, exc_info=True,
            )
        if residual:
            logger.warning(
                'Campaign %s: finalize swept %s residual phase key(s) '
                'contact=%s callid=%s ctx=%s',
                id_campaign, residual, contact_id, callid, context,
            )
        cls._publish_calls_count(id_campaign)
        return True

    @classmethod
    def reset_dialer_calls_counter(cls, id_campaign, reason='', force=False):
        """Reset defensivo de OML:CALLS y fases al finalizar campaña.

        Sin force=True, no resetea si aún hay phase keys / reservas / OML:CALLS
        (evita huérfanos y over-dial tras auto-finalize prematuro).
        """
        if int(id_campaign or 0) == 0:
            return False
        cls.connect_redis_dialer()
        if not force and cls._campaign_has_live_calls(id_campaign):
            phases = cls.get_campaign_channel_phases(id_campaign)
            logger.warning(
                'Campaign %s: skip reset OML:CALLS (live calls/reserves) '
                'ringing=%s waiting=%s oncall=%s (%s)',
                id_campaign,
                phases[PHASE_RINGING], phases[PHASE_WAITING_AGENT],
                phases[PHASE_ONCALL], reason,
            )
            return False
        key_calls = f'OML:CALLS:{id_campaign}:DIALER'
        val = cls.REDIS_DIALER_CONNECTION.get(key_calls)
        try:
            prev = int(val or 0)
        except (TypeError, ValueError):
            prev = 0
        phases = cls.get_campaign_channel_phases(id_campaign)
        if prev > 0 or phases['TOTAL'] > 0:
            logger.warning(
                'Campaign %s: resetting OML:CALLS from %s to 0 '
                '(phases ringing=%s waiting=%s oncall=%s) (%s)',
                id_campaign, prev,
                phases[PHASE_RINGING], phases[PHASE_WAITING_AGENT],
                phases[PHASE_ONCALL], reason,
            )
            cls.REDIS_DIALER_CONNECTION.set(key_calls, 0)
            cls._clear_campaign_channel_phases(id_campaign)
            cls._init_campaign_channel_phases(id_campaign)
            cls._publish_calls_count(id_campaign)
        return True

    @classmethod
    def _count_recent_reserves(cls, id_campaign, grace_sec=None):
        """
        Cantidad de reservas INCR dentro de la ventana de gracia corta
        (en tránsito entre dialer y originate de Asterisk, <= 8s y aún en RINGING).
        """
        grace_sec = grace_sec if grace_sec is not None else min(RESERVE_GRACE_SEC, 8)
        cls.connect_redis_dialer()
        pattern = f'OML:CALLS:RESERVE_TS:{id_campaign}:*'
        prefix = f'OML:CALLS:RESERVE_TS:{id_campaign}:'
        now = int(time.time())
        count = 0
        try:
            for key in cls.REDIS_DIALER_CONNECTION.scan_iter(match=pattern, count=100):
                key_s = cls._decode_redis_str(key) or ''
                contact_id = key_s[len(prefix):] if key_s.startswith(prefix) else ''
                ts_raw = cls.REDIS_DIALER_CONNECTION.get(key)
                if ts_raw is None:
                    continue
                try:
                    if now - int(ts_raw) >= grace_sec:
                        continue
                except (TypeError, ValueError):
                    continue
                if contact_id:
                    phase_key = cls._phase_contact_key(id_campaign, contact_id)
                    ph = cls.REDIS_DIALER_CONNECTION.get(phase_key)
                    if ph is not None and cls._decode_redis_str(ph) != PHASE_RINGING:
                        continue
                count += 1
        except Exception as e:
            logger.debug('Reserve grace count failed camp %s: %s', id_campaign, e)
        return count

    @classmethod
    def _campaign_has_recent_reserve(cls, id_campaign, grace_sec=None):
        """True si hay reserva INCR reciente (evita corrección agresiva en audit)."""
        grace = grace_sec if grace_sec is not None else RESERVE_GRACE_SEC
        cls.connect_redis_dialer()
        pattern = f'OML:CALLS:RESERVE_TS:{id_campaign}:*'
        now = int(time.time())
        try:
            for key in cls.REDIS_DIALER_CONNECTION.scan_iter(match=pattern, count=50):
                ts_raw = cls.REDIS_DIALER_CONNECTION.get(key)
                if ts_raw is None:
                    continue
                try:
                    if now - int(ts_raw) < grace:
                        return True
                except (TypeError, ValueError):
                    continue
        except Exception as e:
            logger.debug('Reserve grace check failed camp %s: %s', id_campaign, e)
        return False

    @classmethod
    def _get_campaign_dialer_status(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor = conn_dialer.cursor()
            cursor.execute(
                'SELECT dialer_status FROM ONLY campaign WHERE id = %s',
                (id_campaign,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return row[0]

    @classmethod
    def _acd_node_heartbeat_key(cls, node_id):
        return f'OML:ACD:NODE:{node_id}:HB'

    @classmethod
    def _acd_node_channels_key(cls, node_id):
        return f'OML:ACD:{node_id}:CHANNELS'

    @classmethod
    def _acd_node_ringing_key(cls, node_id):
        return f'OML:ACD:{node_id}:RINGING'

    @classmethod
    def _acd_node_stale_ticks_key(cls, node_id):
        return f'OML:ACD:NODE:{node_id}:STALE_TICKS'

    @classmethod
    def _evict_stale_acd_nodes(cls, redis_conn, stale_nodes, fresh_nodes):
        """
        Tras ACD_STALE_EVICT_TICKS audits sin HB → SREM de OML:ACD:NODES y
        limpia foto/HB del nodo. Nodos fresh resetean el contador.
        Retorna lista de node_id expulsados.
        """
        try:
            evict_after = max(1, int(ACD_STALE_EVICT_TICKS))
        except (TypeError, ValueError):
            evict_after = 3
        evicted = []
        r = redis_conn
        for node_id in fresh_nodes:
            try:
                r.delete(cls._acd_node_stale_ticks_key(node_id))
            except Exception:
                pass
        for node_id in stale_nodes:
            try:
                ticks_key = cls._acd_node_stale_ticks_key(node_id)
                ticks = int(r.incr(ticks_key))
                r.expire(ticks_key, 3600)
                if ticks < evict_after:
                    continue
                r.srem(ACD_NODES_SET_KEY, node_id)
                r.delete(
                    cls._acd_node_channels_key(node_id),
                    cls._acd_node_ringing_key(node_id),
                    cls._acd_node_heartbeat_key(node_id),
                    ticks_key,
                )
                evicted.append(node_id)
                logger.warning(
                    'ACD node evicted from OML:ACD:NODES: node=%s '
                    'stale_ticks=%s (no heartbeat)',
                    node_id, ticks,
                )
            except Exception:
                logger.exception(
                    'ACD stale eviction failed node=%s', node_id,
                )
        return evicted

    @classmethod
    def _audit_streak_key(cls, camp_id, kind='calls'):
        if kind == 'ringing':
            return f'OML:CALLS:AUDIT:STREAK:RINGING:{camp_id}'
        return f'OML:CALLS:AUDIT:STREAK:{camp_id}'

    @classmethod
    def _decode_redis_str(cls, value):
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value)

    @classmethod
    def _sum_hash_by_camp(cls, hash_data, into):
        """Acumula hash Redis camp_id→count sobre dict int→int."""
        if not hash_data:
            return
        for raw_k, raw_v in hash_data.items():
            try:
                camp_id = int(cls._decode_redis_str(raw_k))
                count = int(cls._decode_redis_str(raw_v) or 0)
            except (TypeError, ValueError):
                continue
            if camp_id <= 0:
                continue
            into[camp_id] = into.get(camp_id, 0) + max(0, count)

    @classmethod
    def _fetch_acd_snapshot_counts(cls):
        """
        Suma fotos publicadas por acd-channel-audit en Redis DB3.

        Retorna (cluster_complete, counts, ringing).
        - NODES vacío o excepción → (False, {}, {}): no reconciliar.
        - Nodo sin heartbeat → no suma su foto; tras ACD_STALE_EVICT_TICKS se
          expulsa de OML:ACD:NODES (desbloquea audit-down).
        - Suma parcial (incompleto con fresh>0) es cota inferior.
        """
        try:
            cls.connect_redis_dialer()
            r = cls.REDIS_DIALER_CONNECTION
            raw_nodes = r.smembers(ACD_NODES_SET_KEY) or set()
            nodes = {
                cls._decode_redis_str(n)
                for n in raw_nodes
                if cls._decode_redis_str(n)
            }
            if not nodes:
                logger.warning(
                    'ACD snapshot audit: OML:ACD:NODES empty; skip reconcile',
                )
                return False, {}, {}

            fresh_nodes = []
            stale_nodes = []
            for node_id in nodes:
                hb = r.get(cls._acd_node_heartbeat_key(node_id))
                if hb is None:
                    stale_nodes.append(node_id)
                else:
                    fresh_nodes.append(node_id)

            if stale_nodes:
                evicted = cls._evict_stale_acd_nodes(r, stale_nodes, fresh_nodes)
                if evicted:
                    stale_set = set(evicted)
                    stale_nodes = [n for n in stale_nodes if n not in stale_set]

            counts = {}
            ringing = {}
            for node_id in fresh_nodes:
                cls._sum_hash_by_camp(
                    r.hgetall(cls._acd_node_channels_key(node_id)) or {},
                    counts,
                )
                cls._sum_hash_by_camp(
                    r.hgetall(cls._acd_node_ringing_key(node_id)) or {},
                    ringing,
                )

            cluster_complete = len(stale_nodes) == 0 and len(fresh_nodes) > 0
            if stale_nodes:
                logger.info(
                    'ACD snapshot audit: incomplete cluster stale=%s fresh=%s',
                    stale_nodes, fresh_nodes,
                )
            elif not fresh_nodes:
                logger.warning(
                    'ACD snapshot audit: no fresh nodes after eviction; '
                    'skip reconcile',
                )
                return False, {}, {}
            return cluster_complete, counts, ringing
        except Exception as e:
            logger.error(
                'Failed to fetch ACD snapshot channel counts: %s', e, exc_info=True,
            )
            return False, {}, {}

    @classmethod
    def _get_audit_streak(cls, camp_id, kind='calls'):
        cls.connect_redis_dialer()
        raw = cls.REDIS_DIALER_CONNECTION.get(cls._audit_streak_key(camp_id, kind))
        if not raw:
            return None
        try:
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8')
            data = json.loads(raw)
            if not isinstance(data, dict):
                return None
            direction = data.get('dir')
            if direction not in (AUDIT_STREAK_DIR_UP, AUDIT_STREAK_DIR_DOWN):
                return None
            return {
                'dir': direction,
                'n': max(0, int(data.get('n') or 0)),
                'observed': max(0, int(data.get('observed') or 0)),
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    @classmethod
    def _clear_audit_streak(cls, camp_id, kind='calls'):
        cls.connect_redis_dialer()
        cls.REDIS_DIALER_CONNECTION.delete(cls._audit_streak_key(camp_id, kind))

    @classmethod
    def _bump_audit_streak(cls, camp_id, direction, observed, kind='calls'):
        """
        Incrementa streak si la dirección continúa; si no, reinicia en 1.
        Para ``up``: reinicia si observed bajó respecto del streak previo.
        Retorna el nuevo n.
        """
        observed = max(0, int(observed))
        prev = cls._get_audit_streak(camp_id, kind)
        if (
            prev
            and prev['dir'] == direction
            and (
                direction != AUDIT_STREAK_DIR_UP
                or observed >= prev['observed']
            )
        ):
            n = prev['n'] + 1
        else:
            n = 1
        payload = json.dumps({
            'dir': direction,
            'n': n,
            'observed': observed,
        })
        cls.REDIS_DIALER_CONNECTION.set(
            cls._audit_streak_key(camp_id, kind), payload, ex=3600,
        )
        return n

    @classmethod
    def _reconcile_campaign_ringing_bucket(
        cls, camp_id, ringing_target, total_target, cluster_complete,
    ):
        """
        Ajusta CAMP:{camp}:CHANNELS RINGING con histéresis asimétrica
        y loguea drift de WAITING_AGENT/ONCALL respecto del total.
        """
        channels_key = cls._channels_hash_key(camp_id)
        phases = cls.get_campaign_channel_phases(camp_id)
        redis_ringing = phases[PHASE_RINGING]
        ringing_target = max(0, int(ringing_target))

        if redis_ringing == ringing_target:
            cls._clear_audit_streak(camp_id, kind='ringing')
        elif redis_ringing < ringing_target:
            n = cls._bump_audit_streak(
                camp_id, AUDIT_STREAK_DIR_UP, ringing_target, kind='ringing',
            )
            if n >= AUDIT_UP_STREAK_TICKS:
                logger.warning(
                    'Audit corrected RINGING camp %s: redis=%s asterisk=%s '
                    '(up streak=%s)',
                    camp_id, redis_ringing, ringing_target, n,
                )
                cls.REDIS_DIALER_CONNECTION.hset(
                    channels_key, PHASE_RINGING, ringing_target,
                )
                cls._clear_audit_streak(camp_id, kind='ringing')
        else:
            # redis_ringing > ringing_target
            recent_reserves = (
                cls._count_recent_reserves(camp_id)
                if cls._campaign_has_recent_reserve(camp_id)
                else 0
            )
            safe_ringing_target = ringing_target + recent_reserves
            if redis_ringing <= safe_ringing_target:
                cls._clear_audit_streak(camp_id, kind='ringing')
                logger.debug(
                    'Audit: skip RINGING camp %s (recent reserve=%s) '
                    'redis=%s asterisk=%s',
                    camp_id, recent_reserves, redis_ringing, ringing_target,
                )
            else:
                down_needed = (
                    AUDIT_DOWN_STREAK_TICKS
                    if cluster_complete
                    else max(1, int(AUDIT_INCOMPLETE_DOWN_STREAK_TICKS))
                )
                n = cls._bump_audit_streak(
                    camp_id, AUDIT_STREAK_DIR_DOWN, safe_ringing_target,
                    kind='ringing',
                )
                if n >= down_needed:
                    logger.warning(
                        'Audit corrected RINGING camp %s: redis=%s asterisk=%s '
                        'recent_reserves=%s -> set %s (down streak=%s '
                        'complete=%s)',
                        camp_id, redis_ringing, ringing_target,
                        recent_reserves, safe_ringing_target, n,
                        cluster_complete,
                    )
                    cls.REDIS_DIALER_CONNECTION.hset(
                        channels_key, PHASE_RINGING, safe_ringing_target,
                    )
                    cls._clear_audit_streak(camp_id, kind='ringing')

        phases_after = cls.get_campaign_channel_phases(camp_id)
        non_ringing = (
            phases_after[PHASE_WAITING_AGENT] + phases_after[PHASE_ONCALL]
        )
        expected_non_ringing = max(0, int(total_target) - int(ringing_target))
        if non_ringing != expected_non_ringing:
            logger.warning(
                'Audit phase drift camp %s: total=%s ringing=%s '
                'waiting=%s oncall=%s expected_non_ringing=%s',
                camp_id, total_target, phases_after[PHASE_RINGING],
                phases_after[PHASE_WAITING_AGENT],
                phases_after[PHASE_ONCALL],
                expected_non_ringing,
            )
            recent = (
                cls._count_recent_reserves(camp_id)
                if cls._campaign_has_recent_reserve(camp_id)
                else 0
            )
            safe_non_ringing = expected_non_ringing + recent
            if cluster_complete and non_ringing > safe_non_ringing:
                excess = non_ringing - safe_non_ringing
                w_cur = phases_after[PHASE_WAITING_AGENT]
                o_cur = phases_after[PHASE_ONCALL]
                dec_w = min(w_cur, excess)
                if dec_w > 0:
                    w_cur -= dec_w
                    excess -= dec_w
                    cls.REDIS_DIALER_CONNECTION.hset(
                        channels_key, PHASE_WAITING_AGENT, w_cur,
                    )
                dec_o = min(o_cur, excess)
                if dec_o > 0:
                    o_cur -= dec_o
                    cls.REDIS_DIALER_CONNECTION.hset(
                        channels_key, PHASE_ONCALL, o_cur,
                    )

    @classmethod
    @timed_lru_cache(
        seconds=int(os.getenv('DIALER_CAMPAIGN_CACHE_TTL_SEC', '15')),
        maxsize=128,
    )
    def get_campaign_max_available_channels(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT max_channels FROM ONLY campaign WHERE id = %s;',
                                  (id_campaign,))
            return cursor_dialer.fetchone()[0]

    @classmethod
    def get_allowed_attempts_according_agents(
        cls, id_campaign, active_channels, campaign_max_available_channels
    ):
        """
        Devuelve cuántos intentos NUEVOS puede iniciar la campaña según:
        - canales ya activos de la campaña
        - canales máximos configurados para la campaña
        - agentes disponibles en la campaña
        (la "justicia" entre campañas ya la maneja allowed_calls_prority_percentage)
        """
        available_agents, total_available_agents = cls.get_number_available_agents(id_campaign)

        logger.debug(
            "Campaign %s: available_agents=%s total_available_agents=%s",
            id_campaign, available_agents, total_available_agents
        )

        # No hay más canales libres configurados para esta campaña
        if active_channels >= campaign_max_available_channels:
            logger.debug(
                "Campaign %s: no free channels (active=%s, max=%s)",
                id_campaign, active_channels, campaign_max_available_channels
            )
            return 0

        # Si no hay agentes para ESTA campaña, no disques
        if available_agents <= 0:
            logger.debug(
                "Campaign %s: no available agents for this campaign (avail=%s)",
                id_campaign, available_agents
            )
            return 0

        # Máximo de canales que me gustaría tener para esta campaña según agentes
        # (1 canal por agente "equivalente")
        desired_total_channels = min(available_agents, campaign_max_available_channels)

        headroom = desired_total_channels - active_channels
        logger.debug(
            "Campaign %s: desired_total_channels=%s headroom=%s",
            id_campaign, desired_total_channels, headroom
        )

        if headroom <= 0:
            logger.debug(
                "Campaign %s: already at desired load (headroom<=0)",
                id_campaign
            )
            return 0

        allowed = int(headroom)
        logger.debug(
            "Campaign %s: allowed_attempts_according_agents=%s",
            id_campaign, allowed
        )
        return allowed

    DIAL_MODE_POWER = 'power'
    DIAL_MODE_PROGRESSIVE = 'progressive'
    DIAL_MODE_PREDICTIVE = 'predictive'

    @classmethod
    @timed_lru_cache(
        seconds=int(os.getenv('DIALER_CAMPAIGN_CACHE_TTL_SEC', '15')),
        maxsize=128,
    )
    def get_boost_factor(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT initial_boost_factor FROM ONLY campaign WHERE id = %s',
                                  (id_campaign,))
            boost_factor = cursor_dialer.fetchone()[0]
            return boost_factor

    @classmethod
    @timed_lru_cache(
        seconds=int(os.getenv('DIALER_CAMPAIGN_CACHE_TTL_SEC', '15')),
        maxsize=128,
    )
    def get_predictive_model(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT initial_predictive_model FROM ONLY campaign WHERE id = %s',
                (id_campaign,))
            row = cursor_dialer.fetchone()
            if not row:
                return False
            return bool(row[0])

    @classmethod
    def _power_dialer_reason(cls, id_campaign):
        """Power si OML:CAMP tiene voicebot o destino custom. None si no."""
        cls.connect_redis_oml()
        customdialerdst, voicebot = cls.REDIS_OML_CONNECTION.hmget(
            f'OML:CAMP:{id_campaign}', 'CUSTOMDIALERDST', 'VOICEBOT',
        )
        if voicebot and str(voicebot).lower() == 'true':
            return 'VOICEBOT=True'
        if customdialerdst not in (None, '', '0'):
            return f'CUSTOMDIALERDST={customdialerdst!r}'
        return None

    # Reason when campaign wants predictive but global FF is off → progressive R=1.
    REASON_PREDICTIVE_FF_DISABLED_R1 = 'predictive_ff_disabled_r1'

    @classmethod
    def resolve_dial_mode(cls, id_campaign):
        """
        Decide automatic dialing mode for a campaign.

        Priority:
          1) power: OML:CAMP CUSTOMDIALERDST not in ('', '0') or VOICEBOT=true
          2) predictive: initial_predictive_model=true AND DIALER_PREDICTIVE_ENABLED
          3) progressive R=1: predictive flag but FF off (ignora boost)
          4) progressive: otherwise (con boost de campaña)
        """
        power_reason = cls._power_dialer_reason(id_campaign)
        if power_reason is not None:
            return cls.DIAL_MODE_POWER, power_reason
        if cls.get_predictive_model(id_campaign):
            if PREDICTIVE_ENABLED:
                return cls.DIAL_MODE_PREDICTIVE, 'initial_predictive_model=True'
            return (
                cls.DIAL_MODE_PROGRESSIVE,
                cls.REASON_PREDICTIVE_FF_DISABLED_R1,
            )
        return cls.DIAL_MODE_PROGRESSIVE, 'initial_predictive_model=False'

    @classmethod
    def _normalize_boost_factor(cls, raw_boost):
        try:
            return float(raw_boost or 1.0)
        except (TypeError, ValueError):
            return 1.0

    @classmethod
    def _allowed_parallel_power(cls, id_campaign, num_available_channels, reason):
        logger.debug(
            "Campaign %s: %s => POWER DIALER mode, "
            "allowed_parallel_contact_attempts=%s",
            id_campaign, reason, num_available_channels
        )
        return num_available_channels

    @classmethod
    def _allowed_parallel_progressive(
            cls, id_campaign, active_channels, campaign_max_available_channels,
            num_available_channels, boost_factor, mode_label='PROGRESSIVE'):
        """
        Progressive pacing: target = A_free * boost_factor.

        Cupo de nuevas originaciones = target − (RINGING + WAITING_AGENT).
        ONCALL no resta: esos canales ya están con agentes ocupados, que
        tampoco entran en A_free. Si se restara OML:CALLS completo, un READY
        quedaría idle mientras otro agente está ONCALL (bug de warm-up /
        throttled / progresivo R=1).

        active_channels sigue usado solo vía num_available_channels
        (headroom max_channels − OML:CALLS).
        """
        available_agents_score, total_agents_available = (
            cls.get_number_available_agents(id_campaign)
        )
        logger.debug(
            "Campaign %s: mode=%s available_agents_score=%s total_agents_available=%s",
            id_campaign, mode_label, available_agents_score, total_agents_available
        )

        if available_agents_score <= 0:
            logger.debug(
                "Campaign %s: no available agents for this campaign, returning 0",
                id_campaign)
            return 0

        active_snap, phases = cls._get_calls_and_phases(id_campaign)
        if active_snap is not None:
            active_channels = active_snap
        raw_unassigned = (
            int(phases.get(PHASE_RINGING, 0) or 0)
            + int(phases.get(PHASE_WAITING_AGENT, 0) or 0)
        )
        max_possible_unassigned = max(
            0, int(active_channels) - int(phases.get(PHASE_ONCALL, 0) or 0),
        )
        unassigned = min(raw_unassigned, max_possible_unassigned)
        target_concurrent_calls = available_agents_score * boost_factor
        target_capped = min(ceil(target_concurrent_calls), campaign_max_available_channels)
        calls_to_dial = target_capped - unassigned

        logger.debug(
            "Campaign %s: mode=%s boost_factor=%s target_concurrent_calls=%s "
            "target_capped=%s unassigned(ringing+waiting)=%s "
            "oncall=%s active_channels=%s calls_to_dial(before caps)=%s",
            id_campaign, mode_label, boost_factor, target_concurrent_calls,
            target_capped, unassigned, phases.get(PHASE_ONCALL, 0),
            active_channels, calls_to_dial
        )

        if calls_to_dial <= 0:
            logger.debug(
                "Campaign %s: already at or above desired load "
                "(calls_to_dial<=0). Returning 0.",
                id_campaign
            )
            return 0

        final_allowed = min(calls_to_dial, num_available_channels)
        final_allowed = max(int(final_allowed), 0)

        logger.debug(
            "Campaign %s: mode=%s final_allowed_parallel_contact_attempts=%s "
            "(after channel cap num_available_channels=%s)",
            id_campaign, mode_label, final_allowed, num_available_channels
        )
        return final_allowed

    @classmethod
    def get_campaign_att_count(cls, id_campaign):
        """Lee ATT_COUNT de CAMP:{id}:ATT (0 si ausente)."""
        if int(id_campaign or 0) == 0:
            return 0
        cls.connect_redis_dialer()
        raw = cls.REDIS_DIALER_CONNECTION.hget(f'CAMP:{id_campaign}:ATT', 'ATT_COUNT')
        try:
            return max(0, int(float(raw or 0)))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def is_predictive_warmup(cls, id_campaign):
        """True mientras ATT_COUNT < WARM_UP_SAMPLE_SIZE (progresivo estricto)."""
        return cls.get_campaign_att_count(id_campaign) < WARM_UP_SAMPLE_SIZE

    @classmethod
    def get_campaign_p_hit(cls, id_campaign):
        """
        P_hit para pacing: EWMA floored por HIT_RATE_FLOOR.
        None si aún no hay muestra (HIT_COUNT + FAIL_COUNT == 0).
        """
        metrics = cls.get_campaign_metrics(id_campaign)
        if not metrics:
            return None
        sample = int(metrics.get('HIT_COUNT') or 0) + int(metrics.get('FAIL_COUNT') or 0)
        if sample <= 0:
            return None
        try:
            p_hit = float(metrics.get('P_HIT') or 0.0)
        except (TypeError, ValueError):
            p_hit = 0.0
        return max(p_hit, HIT_RATE_FLOOR)

    @classmethod
    def get_campaign_drop_rate(cls, id_campaign):
        """
        Drop rate para pacing (γ): DROP_RATE_EWMA (EWMA simétrico).

        Fallback al ratio acumulado DROP_RATE (= ABANDON/HIT) si el hash legacy
        aún no tiene DROP_RATE_EWMA. Retorna None si no hay connects humanos
        (HIT_COUNT=0) ni muestra de EWMA.
        """
        metrics = cls.get_campaign_metrics(id_campaign)
        if not metrics:
            return None
        if metrics.get('HAS_DROP_RATE_EWMA'):
            try:
                return float(metrics.get('DROP_RATE_EWMA'))
            except (TypeError, ValueError):
                return 0.0
        hit_count = int(metrics.get('HIT_COUNT') or 0)
        if hit_count <= 0:
            return None
        try:
            return float(metrics.get('DROP_RATE'))
        except (TypeError, ValueError):
            abandon = int(metrics.get('ABANDON_COUNT') or 0)
            return abandon / float(hit_count)

    @classmethod
    def _publish_campaign_pacing(cls, id_campaign, **fields):
        """
        Publica snapshot de pacing en CAMP:{id}:PACING (DB3) con TTL corto.
        Errores Redis: log + no-op (no debe romper el tick).
        """
        if int(id_campaign or 0) == 0:
            return
        key = f'CAMP:{id_campaign}:PACING'
        mapping = {}
        for name, value in fields.items():
            if value is None:
                mapping[name] = ''
            else:
                mapping[name] = str(value)
        mapping['TS'] = str(int(time.time()))
        try:
            cls.connect_redis_dialer()
            pipe = cls.REDIS_DIALER_CONNECTION.pipeline()
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, PACING_SNAPSHOT_TTL_SEC)
            pipe.execute()
        except Exception:
            logger.exception(
                'Error publishing campaign pacing snapshot camp=%s', id_campaign,
            )

    @classmethod
    def _touch_throttle_state_ttl(cls, id_campaign):
        """Renueva TTL de latch/over_since si el kill-switch sigue activo."""
        if int(id_campaign or 0) == 0:
            return False
        try:
            cls.connect_redis_dialer()
            r = cls.REDIS_DIALER_CONNECTION
            latch_key = f'CAMP:{id_campaign}:THROTTLE_LATCH'
            over_key = f'CAMP:{id_campaign}:THROTTLE_OVER_SINCE'
            if not r.exists(latch_key):
                return False
            ttl = max(1, int(THROTTLE_STATE_TTL_SEC))
            pipe = r.pipeline()
            pipe.expire(latch_key, ttl)
            pipe.expire(over_key, ttl)
            pipe.execute()
            return True
        except Exception:
            logger.debug(
                'Campaign %s: touch throttle TTL failed', id_campaign, exc_info=True,
            )
            return False

    @classmethod
    def _ast_channels_for_campaign(cls, id_campaign):
        """Foto Asterisk agregada (OML:ACD:*:CHANNELS) para una campaña; None si n/d."""
        try:
            _complete, counts, _ringing = cls._fetch_acd_snapshot_counts()
        except Exception:
            logger.debug(
                'Campaign %s: ACD snapshot for guardrail failed',
                id_campaign, exc_info=True,
            )
            return None
        try:
            cid = int(id_campaign)
        except (TypeError, ValueError):
            return None
        if cid not in counts:
            # Sin foto aún: tratar como 0 canales Ast (delta = CALLS).
            return 0
        try:
            return max(0, int(counts.get(cid) or 0))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _caps_batch_should_abort(cls, id_campaign, campaign_max, dials_done=0):
        """
        Revalida mid-CAPS: sin headroom o campaña inactiva → abortar. Tras ≥1
        originate, también si el kill-switch está latched. Siempre renueva TTL
        del latch sin incrementar THROTTLE_STREAK a mitad de lote.
        P0: también aborta por backlog process-event / drift CALLS−Ast / phases.
        """
        try:
            cls.connect_redis_dialer()
            dist_status = cls.REDIS_DIALER_CONNECTION.hget(
                f'CAMP:{id_campaign}:DISTRIBUTION', 'STATUS',
            )
            if dist_status is not None and str(dist_status) == '0':
                return True, 'campaign_inactive'
        except Exception:
            pass
        active, phases = cls._get_calls_and_phases(id_campaign)
        try:
            max_ch = int(campaign_max or 0)
        except (TypeError, ValueError):
            max_ch = 0
        if max_ch > 0 and int(active) >= max_ch:
            return True, 'max_channels'

        try:
            phase_total = int((phases or {}).get('TOTAL') or 0)
        except (TypeError, ValueError):
            phase_total = 0
        block, g_reason = evaluate_backpressure_guardrails(
            active_calls=int(active or 0),
            phase_total=phase_total,
            ast_channels=cls._ast_channels_for_campaign(id_campaign),
        )
        if block:
            return True, g_reason or 'origin_guardrail'

        latched = cls._touch_throttle_state_ttl(id_campaign)
        if int(dials_done or 0) <= 0:
            return False, ''
        if not latched:
            return False, ''
        drop_rate = cls.get_campaign_drop_rate(id_campaign)
        exit_thr = float(THROTTLE_EXIT_RATIO) * float(MAX_ABANDON_RATE)
        if drop_rate is not None and float(drop_rate) < exit_thr:
            throttle = cls._update_throttle_streak(
                id_campaign, drop_rate, MAX_ABANDON_RATE,
            )
            cls._log_throttle_event(id_campaign, throttle, drop_rate)
            if not (throttle.get('force_throttle') or throttle.get('latched')):
                return False, ''
        return True, 'throttle'

    @classmethod
    def _log_throttle_event(cls, id_campaign, throttle, drop_rate):
        """Log THROTTLE_ENGAGED / THROTTLE_CLEARED at warning level."""
        event = (throttle or {}).get('event')
        if event == 'THROTTLE_ENGAGED':
            logger.warning(
                "Campaign %s: THROTTLE_ENGAGED over_sec=%s streak_sec=%s "
                "drop_rate=%s d_max=%s exit_ratio=%s",
                id_campaign,
                throttle.get('over_sec', throttle.get('streak')),
                THROTTLE_STREAK_SEC,
                drop_rate,
                MAX_ABANDON_RATE,
                THROTTLE_EXIT_RATIO,
            )
        elif event == 'THROTTLE_CLEARED':
            logger.warning(
                "Campaign %s: THROTTLE_CLEARED over_sec=%s "
                "drop_rate=%s d_max=%s exit_ratio=%s",
                id_campaign,
                throttle.get('over_sec', throttle.get('streak')),
                drop_rate,
                MAX_ABANDON_RATE,
                THROTTLE_EXIT_RATIO,
            )

    @classmethod
    def _throttle_fail_closed(cls, id_campaign, event=None):
        """Fail-closed: forzar R=1 si Redis falla o el estado es ilegible."""
        return {
            'streak': 0,
            'over_sec': 0,
            'latched': True,
            'force_throttle': True,
            'event': event,
        }

    @classmethod
    def _compute_throttle_drop_rate(cls, metrics, ewma_drop, latched=False):
        """
        D efectivo para kill-switch: max(EWMA, acumulado) si HIT_COUNT >= MIN_HITS.

        Si hits < min y no latched → None (caller no engage; γ sigue con EWMA).
        Si hits < min y latched → max(ewma, cum) para histéresis de salida.
        """
        metrics = metrics or {}
        try:
            hits = int(metrics.get('HIT_COUNT') or 0)
        except (TypeError, ValueError):
            hits = 0
        try:
            min_hits = int(THROTTLE_MIN_HITS)
        except (TypeError, ValueError):
            min_hits = 20
        min_hits = max(0, min_hits)

        cum = None
        try:
            if hits > 0 and metrics.get('DROP_RATE') is not None:
                cum = max(0.0, float(metrics.get('DROP_RATE')))
        except (TypeError, ValueError):
            cum = None

        ewma = None
        if ewma_drop is not None:
            try:
                ewma = max(0.0, float(ewma_drop))
            except (TypeError, ValueError):
                ewma = None

        if hits < min_hits and not latched:
            return None

        vals = [v for v in (ewma, cum) if v is not None]
        if not vals:
            return None
        return max(vals)

    @classmethod
    def _update_throttle_streak(cls, id_campaign, drop_rate, d_max, now_ts=None):
        """
        Actualiza CAMP:{id}:THROTTLE_OVER_SINCE / THROTTLE_LATCH (wall-clock).

        Returns dict:
          streak (= over_sec alias), over_sec, latched, force_throttle, event
          (event in {None, 'THROTTLE_ENGAGED', 'THROTTLE_CLEARED'})
        Fail-closed: errores Redis → force_throttle=True.
        drop_rate=None: no trata D=0 (no limpia latch); lee estado existente.
        """
        empty = {
            'streak': 0,
            'over_sec': 0,
            'latched': False,
            'force_throttle': False,
            'event': None,
        }
        if int(id_campaign or 0) == 0:
            return empty

        over_key = f'CAMP:{id_campaign}:THROTTLE_OVER_SINCE'
        latch_key = f'CAMP:{id_campaign}:THROTTLE_LATCH'
        if now_ts is None:
            now_ts = time.time()
        try:
            now_f = float(now_ts)
        except (TypeError, ValueError):
            now_f = time.time()

        def _over_sec_from_redis(r):
            raw = r.get(over_key)
            try:
                since = float(raw)
            except (TypeError, ValueError):
                return 0
            return max(0, int(now_f - since))

        # Sin muestra de drop: no llamar Lua con d=0 (evitar clear accidental).
        if drop_rate is None:
            try:
                cls.connect_redis_dialer()
                r = cls.REDIS_DIALER_CONNECTION
                latched = bool(r.exists(latch_key))
                over_sec = _over_sec_from_redis(r) if latched or r.exists(over_key) else 0
                return {
                    'streak': over_sec,
                    'over_sec': over_sec,
                    'latched': latched,
                    'force_throttle': latched,
                    'event': None,
                }
            except Exception:
                logger.exception(
                    'Error reading throttle state (drop_rate=None) camp=%s',
                    id_campaign,
                )
                return cls._throttle_fail_closed(id_campaign)

        try:
            d_max_f = float(d_max)
        except (TypeError, ValueError):
            d_max_f = 0.0
        try:
            d = max(0.0, float(drop_rate))
        except (TypeError, ValueError):
            d = 0.0

        try:
            exit_ratio = float(THROTTLE_EXIT_RATIO)
        except (TypeError, ValueError):
            exit_ratio = 0.8
        exit_ratio = max(0.0, min(exit_ratio, 1.0))
        exit_thr = exit_ratio * d_max_f if d_max_f > 0 else 0.0

        try:
            streak_sec = int(THROTTLE_STREAK_SEC)
        except (TypeError, ValueError):
            try:
                streak_sec = int(THROTTLE_STREAK_K)
            except (TypeError, ValueError):
                streak_sec = 5
        streak_sec = max(1, streak_sec)

        try:
            ttl = int(THROTTLE_STATE_TTL_SEC)
        except (TypeError, ValueError):
            ttl = 300
        ttl = max(1, ttl)

        try:
            cls.connect_redis_dialer()
            result = cls.REDIS_DIALER_CONNECTION.eval(
                _THROTTLE_UPDATE_LUA, 2,
                over_key, latch_key,
                d, d_max_f, exit_thr, streak_sec, ttl, now_f,
            )
            over_sec = int(result[0]) if result else 0
            latched = bool(int(result[1])) if result and len(result) > 1 else False
            force_throttle = bool(int(result[2])) if result and len(result) > 2 else False
            event = result[3] if result and len(result) > 3 else ''
            if isinstance(event, bytes):
                event = event.decode('utf-8', errors='replace')
            if not event:
                event = None
            return {
                'streak': over_sec,
                'over_sec': over_sec,
                'latched': latched,
                'force_throttle': force_throttle,
                'event': event,
            }
        except Exception:
            logger.exception(
                'Error updating throttle streak camp=%s', id_campaign,
            )
            return cls._throttle_fail_closed(id_campaign)

    @classmethod
    def _allowed_parallel_predictive(
            cls, id_campaign, active_channels, campaign_max_available_channels,
            num_available_channels):
        """
        Predictive pacing (H5).

        C_dial vía compute_c_dial (predictive_pacer):
          γ > 1: floor(((A_free + A_expected) * γ - C_ringing * P_hit) / P_hit)
          γ ≤ 1: floor(((A_free + A_expected - C_ringing * P_hit) / P_hit) * γ)
        luego apply_channel_caps(C_dial, max_channels - active).

        Warm-up / sin P_hit / kill-switch latched (streak ≥ K): progresivo R=1.
        Aggressiveness = initial_boost_factor (techo de gamma en zona sana).
        """
        snapshot = cls.get_campaign_agent_snapshot(id_campaign)
        # Snapshot CALLS+CHANNELS en un pipeline (misma vista para C_ringing).
        active_snap, phases = cls._get_calls_and_phases(id_campaign)
        if active_snap is not None:
            active_channels = active_snap
        att_count = cls.get_campaign_att_count(id_campaign)
        warmup = att_count < WARM_UP_SAMPLE_SIZE
        drop_rate = cls.get_campaign_drop_rate(id_campaign)
        p_hit = cls.get_campaign_p_hit(id_campaign)
        metrics = cls.get_campaign_metrics(id_campaign) or {}
        capacity = cls.get_campaign_a_expected(id_campaign, snapshot=snapshot)
        aggressiveness = cls._normalize_boost_factor(cls.get_boost_factor(id_campaign))
        raw_ringing = int(phases.get(PHASE_RINGING, 0) or 0)
        waiting_agent = int(phases.get(PHASE_WAITING_AGENT, 0) or 0)
        oncall = int(phases.get(PHASE_ONCALL, 0) or 0)
        max_possible_ringing = max(0, int(active_channels) - waiting_agent - oncall)
        c_ringing = min(raw_ringing, max_possible_ringing)

        throttle = {
            'streak': 0,
            'over_sec': 0,
            'latched': False,
            'force_throttle': False,
            'event': None,
        }
        drop_rate_cum = None
        d_throttle = None
        hit_count_metrics = int(metrics.get('HIT_COUNT') or 0)
        try:
            if metrics.get('DROP_RATE') is not None and hit_count_metrics > 0:
                drop_rate_cum = float(metrics.get('DROP_RATE'))
        except (TypeError, ValueError):
            drop_rate_cum = None

        if not warmup and p_hit is not None:
            # Peek latch without clearing (drop_rate=None path).
            peek = cls._update_throttle_streak(
                id_campaign, None, MAX_ABANDON_RATE,
            )
            d_throttle = cls._compute_throttle_drop_rate(
                metrics, drop_rate, latched=bool(peek.get('latched')),
            )
            if d_throttle is None and peek.get('latched'):
                # Sin D usable: conservar latch (no clear con d=0).
                throttle = peek
            elif d_throttle is None:
                # hits < MIN: no engage; limpia over_since si había.
                throttle = cls._update_throttle_streak(
                    id_campaign, 0.0, MAX_ABANDON_RATE,
                )
            else:
                throttle = cls._update_throttle_streak(
                    id_campaign, d_throttle, MAX_ABANDON_RATE,
                )
            cls._log_throttle_event(id_campaign, throttle, d_throttle)

        decision = decide_predictive_pace(
            a_free=snapshot.get('a_free', 0),
            a_expected=capacity.get('a_expected', 0.0),
            c_ringing=c_ringing,
            p_hit=p_hit,
            drop_rate=drop_rate,
            d_max=MAX_ABANDON_RATE,
            aggressiveness=aggressiveness,
            warmup=warmup,
            p_hit_floor=HIT_RATE_FLOOR,
            gamma_floor=GAMMA_THROTTLE_FLOOR,
            force_throttle=bool(throttle.get('force_throttle')),
            c_waiting=waiting_agent,
            max_dial_ratio=MAX_DIAL_RATIO,
        )
        busy_elapsed_sample = [
            (item['agent_id'], item['status'], item['elapsed_sec'],
             round(item.get('p_lib') or 0.0, 4))
            for item in (capacity.get('details') or [])[:5]
        ]
        mode_label = {
            'warmup': 'PREDICTIVE_WARMUP',
            'predictive': 'PREDICTIVE',
            'throttled': 'PREDICTIVE_THROTTLED',
            'progressive_fallback': 'PREDICTIVE_FALLBACK',
        }.get(decision['mode'], 'PREDICTIVE')

        pacing_common = dict(
            MODE=mode_label,
            REASON=decision['reason'],
            GAMMA=decision['gamma'],
            P_HIT=p_hit,
            DROP_RATE=drop_rate,
            DROP_RATE_CUM=drop_rate_cum if drop_rate_cum is not None else '',
            HIT_COUNT=hit_count_metrics,
            D_THROTTLE=d_throttle if d_throttle is not None else '',
            A_FREE=snapshot.get('a_free', 0),
            A_EXPECTED=capacity.get('a_expected', 0.0),
            C_RINGING=c_ringing,
            C_WAITING=waiting_agent,
            MAX_DIAL_RATIO=MAX_DIAL_RATIO,
            THROTTLE_STREAK=throttle.get('streak', 0),
            THROTTLE_OVER_SEC=throttle.get('over_sec', throttle.get('streak', 0)),
            THROTTLE_LATCHED=1 if throttle.get('latched') else 0,
            EVENT=throttle.get('event') or '',
        )

        if decision['use_progressive_r1']:
            logger.debug(
                "Campaign %s: %s reason=%s a_free=%s a_expected=%s "
                "ringing=%s p_hit=%s drop_rate=%s d_max=%s gamma=%s "
                "att_count=%s aggressiveness=%s streak=%s latched=%s; "
                "progressive R=1",
                id_campaign,
                mode_label,
                decision['reason'],
                snapshot['a_free'],
                capacity.get('a_expected'),
                c_ringing,
                p_hit,
                drop_rate,
                MAX_ABANDON_RATE,
                decision['gamma'],
                att_count,
                aggressiveness,
                throttle.get('streak'),
                throttle.get('latched'),
            )
            c_dial = cls._allowed_parallel_progressive(
                id_campaign,
                active_channels,
                campaign_max_available_channels,
                num_available_channels,
                boost_factor=1.0,
                mode_label=mode_label,
            )
            cls._publish_campaign_pacing(id_campaign, C_DIAL=c_dial, **pacing_common)
            return c_dial

        c_dial_raw = int(decision['c_dial'])
        final_allowed = apply_channel_caps(c_dial_raw, num_available_channels)
        logger.debug(
            "Campaign %s: %s params a_free=%s total_ready=%s "
            "a_busy=%s a_expected=%s t_ring=%s aht=%s att=%s acw=%s "
            "(oncall=%s postcall=%s pause_acw=%s; "
            "totals oncall=%s postcall=%s pause_acw=%s) "
            "channels ringing=%s waiting_agent=%s oncall=%s total=%s "
            "att_count=%s warm_up_sample=%s max_abandon_rate=%s "
            "drop_rate=%s p_hit=%s p_hit_ratio=%s hit=%s fail=%s abandon=%s "
            "gamma=%s aggressiveness=%s c_dial_raw=%s "
            "final_allowed=%s hit_rate_floor=%s predictive_tick_ms=%s "
            "streak=%s latched=%s event=%s "
            "busy_agents=%s p_lib_sample=%s",
            id_campaign,
            mode_label,
            snapshot['a_free'],
            snapshot['total_ready'],
            snapshot['a_busy'],
            capacity.get('a_expected'),
            capacity.get('t_ring'),
            capacity.get('aht'),
            capacity.get('att'),
            capacity.get('acw'),
            snapshot['a_oncall'],
            snapshot['a_postcall'],
            snapshot['a_pause_acw'],
            snapshot['total_oncall'],
            snapshot['total_postcall'],
            snapshot['total_pause_acw'],
            phases[PHASE_RINGING],
            phases[PHASE_WAITING_AGENT],
            phases[PHASE_ONCALL],
            phases['TOTAL'],
            att_count,
            WARM_UP_SAMPLE_SIZE,
            MAX_ABANDON_RATE,
            drop_rate,
            p_hit,
            metrics.get('P_HIT_RATIO'),
            metrics.get('HIT_COUNT'),
            metrics.get('FAIL_COUNT'),
            metrics.get('ABANDON_COUNT'),
            decision['gamma'],
            aggressiveness,
            c_dial_raw,
            final_allowed,
            HIT_RATE_FLOOR,
            PREDICTIVE_TICK_MS,
            throttle.get('streak'),
            throttle.get('latched'),
            throttle.get('event') or '',
            len(snapshot['busy_agents']),
            busy_elapsed_sample,
        )
        cls._publish_campaign_pacing(id_campaign, C_DIAL=final_allowed, **pacing_common)
        return final_allowed

    @classmethod
    def allowed_parallel_contact_attempts(cls, id_campaign):
        """
        Calculates how many NEW calls this campaign can originate in the current cycle.

        Modes (priority order):
          - power: OML:CAMP CUSTOMDIALERDST not in ('', '0') or VOICEBOT=True
              Fill up to max_channels (free channel headroom).
          - predictive: initial_predictive_model=True and DIALER_PREDICTIVE_ENABLED
              C_dial from hit-rate / expected free agents / ringing / gamma.
          - progressive: otherwise
              Desired target = available_agents_score * boost_factor;
              new calls = target − (RINGING + WAITING_AGENT), capped by
              max_channels headroom (ONCALL does not consume READY quota).
        """
        active_channels = cls.get_active_channels(id_campaign)
        campaign_max_available_channels = cls.get_campaign_max_available_channels(id_campaign)

        num_available_channels = campaign_max_available_channels - active_channels
        num_available_channels = max(num_available_channels, 0)

        logger.debug(
            "Campaign %s: active_channels=%s campaign_max_available_channels=%s "
            "num_available_channels=%s",
            id_campaign, active_channels, campaign_max_available_channels, num_available_channels
        )

        dial_mode, reason = cls.resolve_dial_mode(id_campaign)
        logger.debug(
            "Campaign %s: dial_mode=%s reason=%s",
            id_campaign, dial_mode, reason
        )

        if dial_mode == cls.DIAL_MODE_POWER:
            return cls._allowed_parallel_power(
                id_campaign, num_available_channels, reason)

        if dial_mode == cls.DIAL_MODE_PREDICTIVE:
            return cls._allowed_parallel_predictive(
                id_campaign, active_channels, campaign_max_available_channels,
                num_available_channels)

        # Kill-switch global: predictive flag + FF off → R=1 (no boost).
        if reason == cls.REASON_PREDICTIVE_FF_DISABLED_R1:
            return cls._allowed_parallel_progressive(
                id_campaign, active_channels, campaign_max_available_channels,
                num_available_channels, boost_factor=1.0,
                mode_label='PREDICTIVE_DISABLED_R1',
            )

        boost_factor = cls._normalize_boost_factor(cls.get_boost_factor(id_campaign))
        return cls._allowed_parallel_progressive(
            id_campaign, active_channels, campaign_max_available_channels,
            num_available_channels, boost_factor)

    @classmethod
    def take_contacts(cls, contacts_attempts_number, id_campaign):
        logger.debug("Campaign {0}: contacts_attempts_number={1}".format(
            id_campaign, contacts_attempts_number))
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # FOR UPDATE SKIP LOCKED: dos workers concurrentes no eligen
            # la misma fila (red de seguridad junto al lease Redis).
            cursor_dialer.execute("""UPDATE contact_in_campaign as cc
                                     SET status = %s,
                                     schedule_aborted = false
                                     FROM contact as co
                                     WHERE cc.id IN (SELECT id
                                     FROM ONLY contact_in_campaign
                                     WHERE id_campaign = %s and
                                     (status = %s OR schedule_aborted = true)
                                     ORDER BY id
                                     LIMIT %s
                                     FOR UPDATE SKIP LOCKED) AND co.id = cc.id_contact
                                     RETURNING cc.id_contact, cc.id_campaign,
                                     COALESCE(cc.phone_numbers_list[cc.phone_number_index + 1], co.phone);""",
                                  (STATUS_SELECTED_CALL, id_campaign, STATUS_CREATED,
                                   contacts_attempts_number))
            contacts = cursor_dialer.fetchall()
            logger.debug("Campaign {0}: selected {1} contacts".format(id_campaign, len(contacts)))
            return contacts

    @classmethod
    def attempt_contact(cls, contact, id_campaign):
        message = json.dumps({'contact': contact, 'id_campaign': id_campaign})
        # TODO: think if the following could be a background job call
        cls.GM_CLIENT.submit_job('process-contact', message)

    @classmethod
    def is_blacklisted(cls, phone_number):
        """
        Verifica si un número está en la lista negra de OML.
        Retorna True si está bloqueado, False si puede ser llamado.
        """
        if not phone_number:
            return False

        cls.connect_redis_oml()
        black_list_key = 'OML:BLACKLIST'

        try:
            # sismember retorna 1 si existe, 0 si no. En Python bool(1) es True.
            is_black_listed = cls.REDIS_OML_CONNECTION.sismember(
                black_list_key, phone_number
            )
            if is_black_listed:
                logger.warning(
                    f"BLOCKED: Phone number {phone_number} is in BLACKLIST"
                )
            return bool(is_black_listed)
        except Exception as e:
            logger.error(f"Error checking BLACKLIST for {phone_number}: {e}")
            # Ante error de Redis, decidimos si bloquear o permitir.
            # Por seguridad (fail-open vs fail-close), aquí permitimos llamar
            # (False), pero podrías retornar True si prefieres bloquear
            # ante la duda.
            return False

    @classmethod
    @job_handler_decorator
    def process_contact(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        contact = data['contact']
        id_contact = contact[0]
        phone_number = contact[2]

        # --- CHECK BLACKLIST ---
        if cls.is_blacklisted(phone_number):
            logger.info(
                f"Campaign {id_campaign}: Contact {id_contact} "
                f"({phone_number}) skipped (BLACKLIST)"
            )

            # Finalizamos el contacto en la DB para que no se vuelva a intentar
            with cls.get_dialer_connection() as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET final_status = %s, '
                    'status = %s, schedule_aborted = false '
                    'WHERE id_contact = %s AND id_campaign = %s',
                    (FINALIZED_NOCONTACT, STATUS_CHANUNAVAIL, id_contact,
                     id_campaign)
                    # Usamos CHANUNAVAIL o un estatus específico si tuvieras
                    # STATUS_BLACKLISTED
                )

            # Liberamos la reserva que se hizo en el loop
            cls._decrement_calls_once(
                id_campaign, id_contact, None, context='blacklist_skip',
            )

            # Actualizamos Redis para estadísticas
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{id_contact}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT
            )
            return b'Contact skipped: Blacklisted'

        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)

            if status_campaign == ACTIVE:
                if cls.is_allowed_to_call(id_campaign)[0]:

                    # --- NOTA: La reserva ya se hizo en process_campaign_inside ---
                    # Solo necesitamos hacer rollback si falla
                    cls.connect_redis_dialer()

                    try:
                        logger.debug(
                            f'Calling contact {id_contact} '
                            f'(reservation already made)'
                        )
                        # Aplicar prefijo si existe; publicar en cola Gearman
                        # (ACD hace ruta/troncal)
                        phone_to_dial = phone_number
                        prefix = cls.get_prefix(id_campaign)
                        if prefix:
                            try:
                                phone_to_dial = prefix[0] + phone_number
                            except Exception:
                                phone_to_dial = str(prefix) + phone_number
                        dial_attrs = None
                        if ATTEMPT_ID_ENABLED:
                            aid = get_attempt_id(
                                cls.REDIS_DIALER_CONNECTION, id_campaign, id_contact,
                            )
                            if aid:
                                dial_attrs = {'attempt_id': aid}
                        success = cls.trigger_acd_dial(
                            phone_to_dial, id_campaign, id_contact,
                            attributes=dial_attrs,
                        )
                        if not success:
                            raise Exception("Failed to send dial job to Gearman")

                        # Contabilizar el intento exitoso
                        cls.REDIS_DIALER_CONNECTION.hincrby(
                            f'CAMP:{id_campaign}:COUNTER', 'ATTEMPTED_CALLS'
                        )
                        return b'Contact was called'

                    except Exception as e:
                        # --- ROLLBACK DE RESERVA ---
                        cls._decrement_calls_once(
                            id_campaign, id_contact, None,
                            context='gearman_dial_rollback', use_dedup=True,
                        )
                        logger.error(
                            "Error sending dial job to Gearman, "
                            f"reservation rolled back: {e}"
                        )

                        # Marcamos para reintento en DB
                        cursor_dialer.execute(
                            'UPDATE contact_in_campaign SET status = %s '
                            'WHERE id_contact = %s AND id_campaign = %s',
                            (STATUS_CREATED, id_contact, id_campaign)
                        )
                        return b'Contact call failed, marked for retry'
                else:
                    # No está permitido llamar (horario), liberamos la reserva
                    cls._decrement_calls_once(
                        id_campaign, id_contact, None,
                        context='not_allowed_hours', use_dedup=True,
                    )

                    # Marcamos para reintento
                    cursor_dialer.execute(
                        'UPDATE contact_in_campaign SET status = %s '
                        'WHERE id_contact = %s AND id_campaign = %s',
                        (STATUS_CREATED, id_contact, id_campaign)
                    )
                    return b'Contact skipped: Not allowed to call (hours)'
            else:
                # Campaña pausada/finalizada: abortar agenda y liberar reserva.
                # schedule_aborted permite re-seleccionar el contacto al reactivar.
                logger.debug(
                    f'Campaign {id_campaign} is not active '
                    f'(status={status_campaign}), aborting call'
                )
                cls._decrement_calls_once(
                    id_campaign, id_contact, None,
                    context='campaign_not_active', use_dedup=True,
                )
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET schedule_aborted = true '
                    'WHERE id_contact = %s AND id_campaign = %s',
                    (id_contact, id_campaign)
                )
                return b'Aborted call, campaign is not active'

    @classmethod
    @job_handler_decorator
    def pause_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        logger.debug(f'Campaign {id_campaign} pausing the campaign')
        cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=sync_omnileads)
        response = f'Campaign {id_campaign} was paused!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @job_handler_decorator
    def resume_campaign(cls, worker, job):
        cls.connect_redis_oml()
        if not cls.system_is_active():
            cls._notify_dialer_event(json.dumps({'type': 'SYSTEM_STOPPED',
                            'camp_id': 'all'}))
            return b'Forbidden operation'
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        cls.clean_selected_contacts(id_campaign)
        logger.debug(f'Campaign {id_campaign} resuming the campaign')
        cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=sync_omnileads)
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        response = f'Campaign {id_campaign} was resumed!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @job_handler_decorator
    def process_campaign(cls, worker, job):
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        id_campaign = cls.decode_payload(job.data)['id_campaign']
        lock_token = cls.check_running_job(id_campaign)
        if not lock_token:
            return b'Campaign already running'
        logger.debug(f'Campaign {id_campaign}: resuming the campaign')
        try:
            cls.process_campaign_inside(id_campaign, lock_token=lock_token)
            response = f'Campaign {id_campaign} process ended!'
            response = json.dumps({'msg': response})
            return bytes(response, encoding='UTF8')
        except Exception as e:
            raise e
        finally:
            cls._release_process_campaign_lock(id_campaign, lock_token)

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_prefix(cls, id_campaign):
        # Campaña 0: no hay prefijo configurado en BD
        if id_campaign == 0:
            return None
        logger.debug(f'Campaign {id_campaign}: getting prefix')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT prefix FROM campaign WHERE'
                ' id = %s;',
                (id_campaign,))
            row = cursor_dialer.fetchone()
            # fetchone() → (NULL,) es truthy y rompe phone_to_dial con
            # str((None,)) + número. Tratar NULL/vacío como sin prefijo.
            if not row or row[0] is None or row[0] == '':
                return None
            return row

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_incidence_rule(cls, id_campaign, status):
        logger.debug(f'Campaign {id_campaign}: getting the incidence rule for {status}')
        status_code = NAME_TO_STATUS[status]
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT retry_later, max_attempt, in_mode FROM ONLY incidence_rules WHERE'
                ' campaign_id = %s AND status = %s;',
                (id_campaign, status_code))
            return cursor_dialer.fetchone()

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_incidence_rule_disposition(cls, id_campaign, disposition_option):
        logger.debug(f'Campaign {id_campaign}: getting the incidence rule for '
                     f'disposition {disposition_option}')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT retry_later, max_attempt, in_mode FROM ONLY incidence_rules_disposition'
                ' WHERE campaign_id = %s AND disposition_option_id = %s;',
                (id_campaign, disposition_option))
            return cursor_dialer.fetchone()

    @classmethod
    def get_next_phone_number(cls, cursor_dialer, id_campaign, contact_id, phone_number):
        phone_number_index = cls.REDIS_DIALER_CONNECTION.hget(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX')
        if phone_number_index is None:
            # first time: the data is only encoded in Postgres (tables campaign & contact)
            # proceding to decode it
            cursor_dialer.execute('SELECT metadata FROM ONLY campaign WHERE id = %s',
                                  (id_campaign,))
            metadata = json.loads(cursor_dialer.fetchone()[0])
            phone_number_indexes = metadata['cols_telefono'][1:]
            cursor_dialer.execute(
                """SELECT co.data FROM ONLY contact_in_campaign AS cc
                INNER JOIN contact AS co on cc.id_contact = co.id
                WHERE cc.id_campaign = %s AND cc.id_contact = %s;""", (id_campaign, contact_id))
            data = json.loads(cursor_dialer.fetchone()[0])
            phone_number_index = 0
            phone_numbers = [phone_number]
            for i in phone_number_indexes:
                phone_numbers.append(data[i - 1])
            cursor_dialer.execute('UPDATE contact_in_campaign SET phone_numbers_list = %s WHERE'
                                  ' id_campaign = %s AND id_contact = %s;',
                                  (phone_numbers, id_campaign, contact_id))
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST',
                json.dumps(phone_numbers))
        else:
            phone_numbers = json.loads(cls.REDIS_DIALER_CONNECTION.hget(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST'))
        phone_number_index = (int(phone_number_index) + 1) % len(phone_numbers)
        # update Postgres & Redis
        cursor_dialer.execute('UPDATE contact_in_campaign SET phone_number_index = %s'
                              ' WHERE id_campaign = %s AND id_contact = %s;',
                              (phone_number_index, id_campaign, contact_id))
        cls.REDIS_DIALER_CONNECTION.hset(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX', phone_number_index)
        return phone_numbers[phone_number_index]

    @classmethod
    def get_delay(cls, retry_later, type_incidence_rule, attempt_number):
        if type_incidence_rule == FIXED:
            return retry_later
        # MULT
        return retry_later * attempt_number

    @classmethod
    def apply_incidence_rule(
            cls, cursor_dialer, incidence_rule, contact_id, id_campaign, status, status_type,
            phone_number):
        if incidence_rule is not None:
            retry_later, max_attempt, type_incidence_rule = incidence_rule
            contact_history = cls.REDIS_DIALER_CONNECTION.lrange(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}:HISTORY', 0, -1)
            attempt_number = contact_history.count(str((status, status_type)))
            if attempt_number <= max_attempt:
                phone_number = cls.get_next_phone_number(
                    cursor_dialer, id_campaign, contact_id, phone_number)
                retry_later = cls.get_delay(retry_later, type_incidence_rule, attempt_number)
                now_local = datetime.datetime.now()
                datetime_retry_later = now_local + timedelta(seconds=retry_later)

                message = json.dumps({
                    'id_campaign': str(id_campaign),
                    'id_contact': contact_id,
                    'phone_number': phone_number,
                    'datetime_agenda': datetime_retry_later.strftime('%d/%m/%y %H:%M:%S'),
                    'type': 'incidence_rule',
                })
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET final_status = %s '
                    'WHERE id_campaign = %s and id_contact = %s;',
                    (PENDING_ATTEMPTS, id_campaign, contact_id)
                )
                cls.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', PENDING_ATTEMPTS)
                cls.GM_CLIENT.submit_job('schedule-agenda', message)
                return True
            else:
                cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s'
                                      ' WHERE id_campaign = %s and id_contact = %s;',
                                      (FINALIZED_NOCONTACT, id_campaign, contact_id))
                cls.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT)
        else:
            cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                  ' id_campaign = %s and id_contact = %s;',
                                  (FINALIZED_NOCONTACT, id_campaign, contact_id))
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT)
        return False

    @classmethod
    def handle_incidence_rules(cls, status, id_campaign, contact_id, phone_number):
        # if there is an incidence rule for the status:
        #   if the contact's history and the incidence rule indicates that the
        #   contact must be called again, schedule a call according to the incidence rule
        cls.connect_redis_dialer()
        # Count attempts with the real dial status (AMD stays AMD in HISTORY).
        history_status_code = NAME_TO_STATUS[status]
        # Rule lookup may alias (AMD → TERMINATED / Contestador in OML UI).
        rule_status = INCIDENCE_RULE_EVENT_MAP.get(status, status)
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            incidence_rule = cls.get_incidence_rule(id_campaign, rule_status)
            cls.apply_incidence_rule(
                cursor_dialer, incidence_rule, contact_id, id_campaign,
                history_status_code, PHONE_TYPE, phone_number)

    @classmethod
    @job_handler_decorator
    def process_event(cls, worker, job):
        ari_event_data = cls.decode_payload(job.data)
        event_type = ari_event_data.get('type', 'unknown')
        call_type = ari_event_data.get('call_type', '')
        dialstatus = ari_event_data.get('dialstatus', '')
        dialstring = ari_event_data.get('dialstring', '')
        callid = ari_event_data.get('callid') or ari_event_data.get('uniqueid') or ''

        # H7: solo métrica AMD; sin contact_id / DECR / send-reports.
        if event_type == 'AmdLatency':
            id_campaign = ari_event_data.get('id_campaign')
            raw_amd = ari_event_data.get('amd_duration')
            if int(id_campaign or 0) == 0 or raw_amd is None:
                logger.debug(
                    "process_event [%s]: AmdLatency no-op | campaign=%s amd_duration=%s",
                    callid, id_campaign, raw_amd,
                )
                return b'Event was processed'
            try:
                amd_duration = float(raw_amd)
            except (TypeError, ValueError):
                logger.debug(
                    "process_event [%s]: AmdLatency amd_duration inválido %r",
                    callid, raw_amd,
                )
                return b'Event was processed'
            if amd_duration < 0:
                amd_duration = 0.0
            cls._update_campaign_amd_latency(id_campaign, amd_duration)
            logger.info(
                "process_event [%s]: AmdLatency | campaign=%s amd_duration=%s",
                callid, id_campaign, amd_duration,
            )
            return b'Event was processed'

        try:
            id_campaign, contact_id, phone_number = cls.get_contact_data(ari_event_data)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(
                "process_event [%s]: payload sin campos explícitos de negocio "
                "(id_campaign/contact_id/phone_number) | type=%s call_type=%s "
                "error=%s payload_keys=%s",
                callid, event_type, call_type, e,
                list(ari_event_data.keys()) if ari_event_data else [],
            )
            raise

        attempt_id = ari_event_data.get('attempt_id') or ''
        if attempt_id:
            logger.debug(
                "process_event [%s]: attempt_id=%s (M0 echo; cupo sigue "
                "contact/callid)",
                callid, attempt_id,
            )
        logger.info(
            "process_event [%s]: recibido | campaign=%s contact=%s phone=%s "
            "type=%s call_type=%s dialstatus=%s dialstring=%s attempt_id=%s",
            callid, id_campaign, contact_id, phone_number,
            event_type, call_type, dialstatus, dialstring, attempt_id or '-',
        )

        # ----- RouteValidationFailed (llamada bloqueada por validación de ruta): decrementar -----
        if event_type == 'RouteValidationFailed':
            if int(id_campaign or 0) == 0:
                logger.debug(
                    "process_event [%s]: RouteValidationFailed ignorado (campaña 0)",
                    callid,
                )
                return b'Event was processed'
            cls.connect_redis_dialer()
            cls._decrement_calls_once(
                id_campaign, contact_id, callid, context='RouteValidationFailed',
            )
            cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
            logger.info(
                "process_event [%s]: RouteValidationFailed, decrement | "
                "campaign=%s contact=%s phone=%s",
                callid, id_campaign, contact_id, phone_number,
            )
            return b'Event was processed'

        # ----- ChannelDestroyed (canal PSTN liberado): decrementar y reportar -----
        # Tras Dial AMD / otros CALLS_DECR_DIAL_STATUSES el DECR ya ocurrió: dedup
        # → no-op y no re-encolar send-reports (fan-out AMD).
        if event_type in ('ChannelDestroyed', 'ChannelDestroy'):
            if call_type == 'to_pstn' and int(id_campaign or 0) != 0:
                decremented = cls._decrement_calls_once(
                    id_campaign, contact_id, callid, context='ChannelDestroyed',
                )
                if decremented:
                    cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
                    logger.debug(
                        "process_event [%s]: ChannelDestroyed to_pstn, "
                        "decrement y send-reports | campaign=%s",
                        callid, id_campaign,
                    )
                else:
                    logger.debug(
                        "process_event [%s]: ChannelDestroyed to_pstn, "
                        "skip send-reports (cupo ya liberado) | campaign=%s",
                        callid, id_campaign,
                    )
            else:
                logger.warning(
                    "process_event [%s]: ChannelDestroyed ignorado (call_type=%s o campaña 0)",
                    callid, call_type,
                )
            return b'Event was processed'

        # ----- Dial: solo eventos terminales (ANSWER o fallo);
        # intermedios (vacío, RINGING) se ignoran -----
        if event_type != 'Dial':
            logger.debug("process_event [%s]: tipo %s no manejado, omitiendo", callid, event_type)
            return b'Event was processed'

        # Intermedios: no actualizar estado ni enviar send-reports
        if dialstatus in ('', 'RINGING') or dialstatus is None:
            logger.debug(
                "process_event [%s]: Dial intermedio (dialstatus=%r), "
                "sin actualizar estado ni send-reports",
                callid, dialstatus,
            )
            return b'Event was processed'

        # P0: AmdLatency embebido en Dial AMD (evita job Gearman extra desde ACD).
        if dialstatus == 'AMD' and ari_event_data.get('amd_duration') is not None:
            if int(id_campaign or 0) != 0:
                try:
                    amd_duration = float(ari_event_data.get('amd_duration'))
                    if amd_duration < 0:
                        amd_duration = 0.0
                    cls._update_campaign_amd_latency(id_campaign, amd_duration)
                    logger.info(
                        "process_event [%s]: Dial AMD + amd_duration | "
                        "campaign=%s amd_duration=%s",
                        callid, id_campaign, amd_duration,
                    )
                except (TypeError, ValueError):
                    logger.debug(
                        "process_event [%s]: Dial AMD amd_duration inválido %r",
                        callid, ari_event_data.get('amd_duration'),
                    )

        # EXIT_ANSWERED: ATT de campaña; SIN_DISPOSICION si el contacto no calificó.
        if dialstatus == 'EXIT_ANSWERED':
            try:
                agent_duration = float(ari_event_data.get('agent_duration', 0) or 0)
            except (TypeError, ValueError):
                agent_duration = 0.0
            if agent_duration < 0:
                agent_duration = 0.0
            if int(id_campaign or 0) != 0:
                cls._update_campaign_att(id_campaign, agent_duration)
                cls._incr_sin_disposicion_if_unqualified(id_campaign, contact_id)
                logger.info(
                    "process_event [%s]: EXIT_ANSWERED ATT | campaign=%s "
                    "contact=%s agent_duration=%s",
                    callid, id_campaign, contact_id, agent_duration,
                )
            else:
                logger.debug(
                    "process_event [%s]: EXIT_ANSWERED ignorado (campaña 0)",
                    callid,
                )
            cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
            return b'Event was processed'

        # EXIT_ACW: solo ACW de campaña (acw_duration); no status ni DECR.
        if dialstatus == 'EXIT_ACW':
            raw_acw = ari_event_data.get('acw_duration')
            if raw_acw is None:
                logger.debug(
                    "process_event [%s]: EXIT_ACW sin acw_duration, no-op | campaign=%s",
                    callid, id_campaign,
                )
            elif int(id_campaign or 0) != 0:
                try:
                    acw_duration = float(raw_acw)
                except (TypeError, ValueError):
                    acw_duration = None
                if acw_duration is not None:
                    if acw_duration < 0:
                        acw_duration = 0.0
                    cls._update_campaign_acw(id_campaign, acw_duration)
                    logger.info(
                        "process_event [%s]: EXIT_ACW ACW | campaign=%s "
                        "acw_duration=%s",
                        callid, id_campaign, acw_duration,
                    )
            else:
                logger.debug(
                    "process_event [%s]: EXIT_ACW ignorado (campaña 0)",
                    callid,
                )
            cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
            return b'Event was processed'

        # Pierna to_agent: solo ANSWER actualiza status (éxito). Falls CANCEL/NOANSWER
        # no deben pisar EXIT_ABANDON/EXIT_TIMEOUT ni marcar TERMINATED al cancelar ring.
        if call_type == 'to_agent' and not cls.is_answer_event(ari_event_data):
            logger.debug(
                "process_event [%s]: Dial to_agent no-ANSWER ignorado | "
                "campaign=%s contact=%s dialstatus=%s",
                callid, id_campaign, contact_id, dialstatus,
            )
            cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
            return b'Event was processed'

        if cls.is_answer_event(ari_event_data):
            if cls.is_answered_pstn(ari_event_data):
                status = "ANSWERED_PSTN"
                logger.info(
                    "process_event [%s]: ANSWERED_PSTN (troncal) | campaign=%s contact=%s phone=%s",
                    callid, id_campaign, contact_id, phone_number,
                )
                cls.set_contact_status(id_campaign, contact_id, status)
                cls.update_campaign_hit(id_campaign, hit=True)
                if int(id_campaign or 0) != 0:
                    cls._transition_channel_phase(
                        id_campaign, contact_id, callid, PHASE_WAITING_AGENT,
                    )
                # ART: ring_duration opcional (ACD); sin campo = no-op (compatible ACD viejo).
                raw_ring = ari_event_data.get('ring_duration')
                if raw_ring is not None and int(id_campaign or 0) != 0:
                    try:
                        ring_duration = float(raw_ring)
                    except (TypeError, ValueError):
                        ring_duration = None
                    if ring_duration is not None:
                        if ring_duration < 0:
                            ring_duration = 0.0
                        cls._update_campaign_art(id_campaign, ring_duration)
                        logger.info(
                            "process_event [%s]: ANSWERED_PSTN ART | campaign=%s "
                            "contact=%s ring_duration=%s",
                            callid, id_campaign, contact_id, ring_duration,
                        )
            elif cls.is_answered_agent(ari_event_data):
                status = "ANSWERED_AGENT"
                logger.info(
                    "process_event [%s]: ANSWERED_AGENT (cola) | campaign=%s contact=%s phone=%s",
                    callid, id_campaign, contact_id, phone_number,
                )
                cls.set_contact_status(id_campaign, contact_id, status)
                if int(id_campaign or 0) != 0:
                    cls._transition_channel_phase(
                        id_campaign, contact_id, callid, PHASE_ONCALL,
                    )
                cls.connect_redis_dialer()
                with cls.get_dialer_connection() as conn_dialer:
                    cursor_dialer = conn_dialer.cursor()
                    cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                          ' id_campaign = %s and id_contact = %s;',
                                          (FINALIZED_SUCCESS, id_campaign, contact_id))
                    cls.REDIS_DIALER_CONNECTION.hset(
                        f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_SUCCESS)
                logger.info(
                    "process_event [%s]: contacto finalizado con éxito | "
                    "campaign=%s contact=%s phone=%s",
                    callid, id_campaign, contact_id, phone_number,
                )
        elif cls.is_fail_event(ari_event_data):
            fail_status = cls.decode_fail_event(ari_event_data)
            logger.info(
                "process_event [%s]: evento fallo (Dial) | campaign=%s "
                "contact=%s phone=%s dialstatus=%s decoded=%s",
                callid, id_campaign, contact_id, phone_number,
                dialstatus, fail_status,
            )
            cls.handle_fail_event(ari_event_data, id_campaign, contact_id, phone_number)
            if fail_status in ABANDON_STATUSES:
                cls.update_campaign_hit(id_campaign, hit=False, abandon=True)
            elif fail_status in TECHNICAL_FAIL_STATUSES:
                cls.update_campaign_hit(id_campaign, hit=False, tech=True)
            elif fail_status in FAIL_HIT_STATUSES:
                cls.update_campaign_hit(id_campaign, hit=False, abandon=False)
            # Solo liberar cupo por pierna PSTN (no por Dial CANCEL/etc. de agente)
            if (
                dialstatus in CALLS_DECR_DIAL_STATUSES
                and call_type == 'to_pstn'
                and int(id_campaign or 0) != 0
            ):
                cls._decrement_calls_once(
                    id_campaign, contact_id, callid, context=f'Dial {dialstatus}',
                )
                logger.debug(
                    "process_event [%s]: Dial %s to_pstn, decrement | campaign=%s",
                    callid, dialstatus, id_campaign,
                )
        else:
            logger.debug(
                "process_event [%s]: Dial no answer ni fail (ignorado) | "
                "campaign=%s contact=%s dialstatus=%s",
                callid, id_campaign, contact_id, dialstatus,
            )

        cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
        logger.debug(
            "process_event [%s]: finalizado, job send-reports enviado | campaign=%s contact=%s",
            callid, id_campaign, contact_id,
        )
        return b'Event was processed'

    @classmethod
    def is_fail_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        type_event = ari_event_data.get('type')
        return type_event == 'Dial' and  \
            ((dialstatus in FAIL_EVENTS) or (dialstatus in FAIL_NO_RULES_EVENTS))

    @classmethod
    def decode_fail_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        if dialstatus != "NOANSWER":
            return dialstatus
        dialstring = ari_event_data.get('dialstring') or ''
        pattern_timeout = r'^camp_\d+@omlacd$'
        if re.match(pattern_timeout, dialstring):
            return "TIMEOUT"
        return "NOANSWER"

    @classmethod
    def _finalize_contact_nocontact(cls, id_campaign, contact_id, cursor=None):
        """Marca final_status = FINALIZED_NOCONTACT en Postgres y Redis."""
        if int(id_campaign or 0) == 0 or int(contact_id or 0) == 0:
            return
        if cursor is None:
            with cls.get_dialer_connection() as conn_dialer:
                cur = conn_dialer.cursor()
                cur.execute(
                    'UPDATE contact_in_campaign SET final_status = %s '
                    'WHERE id_campaign = %s AND id_contact = %s;',
                    (FINALIZED_NOCONTACT, id_campaign, contact_id),
                )
        else:
            cursor.execute(
                'UPDATE contact_in_campaign SET final_status = %s '
                'WHERE id_campaign = %s AND id_contact = %s;',
                (FINALIZED_NOCONTACT, id_campaign, contact_id),
            )
        cls.connect_redis_dialer()
        cls.REDIS_DIALER_CONNECTION.hset(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}',
            'STATUS',
            FINALIZED_NOCONTACT,
        )

    @classmethod
    def handle_fail_event(cls, ari_event_data, id_campaign, contact_id, phone_number):
        event = cls.decode_fail_event(ari_event_data)
        logger.debug(f'Campaign {id_campaign}: receiving {event} for contact {contact_id}')
        cls.set_contact_status(id_campaign, contact_id, event)
        if event in FAIL_EVENTS and event not in FAIL_NO_RULES_EVENTS:
            cls.handle_incidence_rules(event, id_campaign, contact_id, phone_number)
        else:
            cls._finalize_contact_nocontact(id_campaign, contact_id)

    @classmethod
    def is_answer_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        type_event = ari_event_data.get('type')
        return type_event == 'Dial' and dialstatus == 'ANSWER'

    @classmethod
    def get_contact_data(cls, ari_event_data):
        """
        Extrae id_campaign, contact_id y phone_number desde campos explícitos del payload.

        Contrato requerido:
        - id_campaign
        - contact_id
        - phone_number
        """
        required_fields = ('id_campaign', 'contact_id', 'phone_number')
        missing_fields = [field for field in required_fields if field not in ari_event_data]
        if missing_fields:
            raise KeyError(f"Missing required fields: {missing_fields}")

        id_campaign = ari_event_data.get('id_campaign')
        contact_id = ari_event_data.get('contact_id')
        phone_number = ari_event_data.get('phone_number')

        if id_campaign in (None, "") or contact_id in (None, "") or phone_number in (None, ""):
            raise ValueError(
                "Invalid empty values in required fields: "
                f"id_campaign={id_campaign!r}, "
                f"contact_id={contact_id!r}, "
                f"phone_number={phone_number!r}"
            )

        return str(id_campaign), str(contact_id), str(phone_number)

    @classmethod
    def is_answered_pstn(cls, ari_event_data):
        """
        Indica si la respuesta fue PSTN (troncal).
        Usa call_type cuando el ACD lo envía (to_pstn / to_agent); fallback a dialstring.
        """
        call_type = ari_event_data.get('call_type')
        if call_type == 'to_pstn':
            return True
        if call_type == 'to_agent':
            return False
        dialstring = ari_event_data.get('dialstring') or ''
        return dialstring.find('camp_') == -1

    @classmethod
    def is_answered_agent(cls, ari_event_data):
        """
        Indica si la respuesta fue agente (cola).
        Usa call_type cuando el ACD lo envía; fallback a dialstring.
        """
        call_type = ari_event_data.get('call_type')
        if call_type == 'to_agent':
            return True
        if call_type == 'to_pstn':
            return False
        dialstring = ari_event_data.get('dialstring') or ''
        return dialstring.find('camp_') >= 0

    @classmethod
    def set_contact_status(cls, id_campaign, contact_id, status, type_status=PHONE_TYPE):
        # Campaña 0 o contacto 0: ignorar eventos de estado (casos de sistema)
        if int(id_campaign) == 0 or int(contact_id or 0) == 0:
            logger.info(f"SYSTEM EVENT: Status {status} ignored for ID 0")
            return
        status_code = NAME_TO_STATUS[status]
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if status_code == STATUS_ANSWERED_PSTN:
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status_pstn = true, '
                    'history = array_append(history, %s)'
                    ' WHERE id_campaign = %s AND id_contact = %s;',
                    (str((status_code, type_status)), id_campaign, contact_id))
            else:
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status = %s, '
                    'history = array_append(history, %s)'
                    ' WHERE id_campaign = %s AND id_contact = %s;',
                    (status_code, str((status_code, type_status)), id_campaign, contact_id))
            cls.connect_redis_dialer()
            cls.REDIS_DIALER_CONNECTION.rpush(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}:HISTORY', str((status_code, type_status)))
            cls.REDIS_DIALER_CONNECTION.hincrby(
                f'CAMP:{id_campaign}:COUNTER',
                status
            )

    @classmethod
    @job_handler_decorator
    def delete_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Removing campaign with id = {id_campaign}')
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        was_in_set = cls.REDIS_DIALER_CONNECTION.srem(cls.ACTIVE_CAMPAIGNS_SET, id_campaign)
        if was_in_set:
            cls.update_percentages_priority_campaigns()
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM campaign WHERE id = %s;', (id_campaign,))
            cls.REDIS_DIALER_CONNECTION.delete(f'OML:CALLS:{id_campaign}:DIALER')
            cls._clear_campaign_channel_phases(id_campaign)
            cls.update_percentages_priority_campaigns()
            # TODO: remove the remaining data in Redis
            cls._notify_dialer_event(json.dumps({'type': 'DELETE',
                            'camp_id': id_campaign}))
        try:
            cls.get_boost_factor.cache_clear()
            cls.get_predictive_model.cache_clear()
            cls.get_campaign_max_available_channels.cache_clear()
            cls.get_incidence_rule.cache_clear()
            cls.get_incidence_rule_disposition.cache_clear()
            cls.get_prefix.cache_clear()
        except AttributeError:
            pass
        return b'Campaign was deleted'

    @classmethod
    def set_campaign_status(cls, id_campaign, new_status, cursor=None, sync_omnileads=False):
        """
        Centralized method for changing a campaign's state.
        It updates the DB, the state in Redis, and triggers the global percentage recalculation.
        """
        is_now_active = (new_status == ACTIVE)

        # 1. Upgrade DB
        if not sync_omnileads:
            if cursor is None:
                with cls.get_dialer_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'UPDATE campaign SET '
                        'dialer_status = %s WHERE id = %s;', (new_status, id_campaign)
                    )
            else:
                cursor.execute(
                    'UPDATE campaign SET '
                    'dialer_status = %s WHERE id = %s;', (new_status, id_campaign)
                )
        else:
            with cls.get_dialer_connection() as conn_dialer, conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'UPDATE campaign SET '
                        'dialer_status = %s WHERE id = %s;', (new_status, id_campaign)
                    )
                    cursor_oml.execute(
                        'UPDATE ominicontacto_app_campana '
                        'SET estado = %s WHERE id = %s;', (new_status, id_campaign)
                    )

        # 2. Update Redis status directly
        cls.connect_redis_dialer()
        redis_key = f'CAMP:{id_campaign}:DISTRIBUTION'
        cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'STATUS', '1' if is_now_active else '0')
        if is_now_active:
            cls.REDIS_DIALER_CONNECTION.sadd(cls.ACTIVE_CAMPAIGNS_SET, id_campaign)
        else:
            cls.REDIS_DIALER_CONNECTION.srem(cls.ACTIVE_CAMPAIGNS_SET, id_campaign)

        # 3. Trigger global recalculation. This function will handle any necessary backfill.
        cls.update_percentages_priority_campaigns()

        # 4. Publish the change (logic unchanged)
        cls.connect_redis_oml()
        cls._notify_dialer_event(json.dumps({'type': 'STATUSCHANGE',
                        'camp_id': id_campaign,
                        'status': new_status,
                        'admin': AdminRender.render_status_change(
                            id_campaign, new_status, CAMPAIGN_STATUS_TO_NAME[new_status],
                            AVAILABLE_NEXT_STATUSES[new_status])}))

    @classmethod
    @job_handler_decorator
    def stop_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        logger.debug(f'Stopping campaign with id = {id_campaign}')
        # Cambiar estado a FINALIZED antes de limpiar cupo para cortar nuevos originates
        cls.set_campaign_status(id_campaign, FINALIZED, sync_omnileads=sync_omnileads)
        cls.reset_dialer_calls_counter(
            id_campaign, reason='stop_campaign', force=True,
        )
        return b'Campaign was finalized'

    @classmethod
    def update_prev_stats(cls, id_campaign, stats=None):
        """Copy the current stats to the key 'COUNTER_PREV' so it can be used for comparison
        and send only the modified keys"""
        snapshot = stats if stats is not None else cls.REDIS_DIALER_CONNECTION.hgetall(
            f'CAMP:{id_campaign}:COUNTER')
        for key, value in snapshot.items():
            cls.REDIS_DIALER_CONNECTION.hset(f'CAMP:{id_campaign}:COUNTER_PREV', key, value)

    @classmethod
    @job_handler_decorator
    def send_reports(cls, worker, job):
        cls.connect_redis_dialer()
        ari_event_data = cls.decode_payload(job.data)
        id_campaign, contact_id, phone_number = cls.get_contact_data(ari_event_data)

        # GUARDIA: Si es campaña de sistema (ID 0), no generamos reportes de base de datos
        if int(id_campaign) == 0:
            return b'Success: System campaign reports skipped'

        previous_stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER_PREV') \
            or {}
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """SELECT COUNT(*) FROM ONLY contact_in_campaign WHERE id_campaign = %s
                and (status = %s or status = %s);""",
                (id_campaign, STATUS_CREATED, STATUS_SELECTED_CALL))
            pending_for_call = cursor_dialer.fetchone()[0]
            cursor_dialer.execute("""SELECT final_status, COUNT(*) FROM ONLY contact_in_campaign
            WHERE id_campaign = %s and final_status <> %s GROUP BY final_status;""",
                                  (id_campaign, INITIAL))

            final_statuses = {
                PENDING_ATTEMPTS: 0,
                FINALIZED_NOCONTACT: 0,
                FINALIZED_SUCCESS: 0
            }
            for final_status_label, final_status_value in cursor_dialer.fetchall():
                final_statuses[final_status_label] = final_status_value

            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[PENDING_ATTEMPTS],
                final_statuses[PENDING_ATTEMPTS]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[FINALIZED_NOCONTACT],
                final_statuses[FINALIZED_NOCONTACT]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[FINALIZED_SUCCESS],
                final_statuses[FINALIZED_SUCCESS]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                'PENDING_INITIAL_CONTACT_ATTEMPTS',  # pending to be contacted for the first time
                pending_for_call
            )
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            logger.debug(f'Report for campaign {id_campaign}: {stats}')
            cls.connect_redis_oml()
            cls._notify_dialer_event(json.dumps({
                    'type': 'EVENT',
                    'camp_id': id_campaign,
                    'data': ari_event_data
                }))
            stats_message = {
                'type': 'STATS',
                'camp_id': id_campaign,
            }
            # for Redis PUBSUB
            changed_stats = dict(set(stats.items()) - set(previous_stats.items()))
            changed_stats.update(stats_message)
            changed_stats.update({'admin': AdminRender.render_stats_inner(
                id_campaign, stats)})
            changed_stats_json = json.dumps(changed_stats)
            cls._notify_dialer_event(changed_stats_json)
            cls.update_prev_stats(id_campaign, stats=stats)
            # for Postgres in Omnidialer
            stats_json = json.dumps(stats)
            cursor_dialer.execute(
                'UPDATE campaign SET statistics = %s WHERE id = %s;', (stats_json, id_campaign))
            return b'Success!'

    @classmethod
    def handle_disposition_option(cls, data):
        id_campaign = data['id_campaign']
        id_contact = data['id_contact']
        disposition_option = data['disposition_option']
        logger.debug(f'Adding disposition option {disposition_option} to contact {id_contact}'
                     f' in campaign {id_campaign}')
        cls.connect_redis_dialer()
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """UPDATE contact_in_campaign as cc
                SET disposition_option = %s, history = array_append(history, %s)
                FROM contact AS co
                WHERE cc.id_campaign = %s AND cc.id_contact = %s AND co.id = cc.id_contact
                RETURNING co.phone;""",
                (disposition_option, str((disposition_option, DISPOSITION_TYPE)),
                 id_campaign, id_contact))
            phone_number = cursor_dialer.fetchone()[0]
            cls.REDIS_DIALER_CONNECTION.rpush(
                f'CONTACT:{id_contact}:CAMP:{id_campaign}:HISTORY',
                str((disposition_option, DISPOSITION_TYPE)))
            cls.connect_redis_dialer()
            incidence_rule = cls.get_incidence_rule_disposition(id_campaign, disposition_option)
            incidence_rule_applied = cls.apply_incidence_rule(
                cursor_dialer, incidence_rule, id_contact, id_campaign, disposition_option,
                DISPOSITION_TYPE, phone_number)
            status = (
                cls.get_campaign_status(id_campaign, cursor_dialer)
                if incidence_rule_applied else None
            )
        # if the incidence rule was applied and the campaign is paused/finalized, reactivate after commit
        if incidence_rule_applied and status in (PAUSED, FINALIZED):
            cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=True)
            message = json.dumps({'id_campaign': id_campaign})
            cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        return b'Disposition for incidence rule was added!'

    @classmethod
    def _acquire_audit_lock(cls, ttl_sec=None):
        """Toma lock Redis NX. Retorna token si se adquirió; None si está ocupado."""
        ttl_sec = ttl_sec if ttl_sec is not None else CHANNEL_AUDIT_LOCK_TTL_SEC
        if ttl_sec <= 0:
            ttl_sec = 55
        cls.connect_redis_dialer()
        token = str(uuid.uuid4())
        acquired = cls.REDIS_DIALER_CONNECTION.set(
            AUDIT_LOCK_KEY, token, nx=True, ex=ttl_sec,
        )
        if acquired:
            return token
        return None

    @classmethod
    def _release_audit_lock(cls, token):
        """Libera el lock sólo si el token sigue siendo el nuestro."""
        if not token:
            return False
        cls.connect_redis_dialer()
        try:
            released = cls.REDIS_DIALER_CONNECTION.eval(
                _AUDIT_UNLOCK_LUA, 1, AUDIT_LOCK_KEY, token,
            )
            return bool(released)
        except Exception as e:
            logger.warning('Audit lock release failed: %s', e)
            return False

    @classmethod
    def audit_active_channels(cls):
        """
        Reconcilia OML:CALLS:{camp}:DIALER con la suma de fotos ACD en Redis DB3.

        Histéresis asimétrica: subir tras AUDIT_UP_STREAK_TICKS (cota inferior OK
        con cluster incompleto). Bajar: cluster completo con AUDIT_DOWN_STREAK_TICKS,
        o incompleto (fresh>0) con AUDIT_INCOMPLETE_DOWN_STREAK_TICKS. Nodos sin HB
        se expulsan de OML:ACD:NODES tras ACD_STALE_EVICT_TICKS. NODES vacío ≠ cero.
        """
        logger.info("Iniciando auditoría de canales activos (Sanity Check)...")

        try:
            cluster_complete, asterisk_counts, asterisk_ringing = (
                cls._fetch_acd_snapshot_counts()
            )
            # NODES vacío / error de lectura → counts vacíos y incomplete.
            # Sin nodos registrados no hay señal confiable: no tocar.
            cls.connect_redis_dialer()
            nodes = cls.REDIS_DIALER_CONNECTION.smembers(ACD_NODES_SET_KEY) or set()
            if not nodes:
                logger.warning(
                    "Auditoría omitida: sin nodos ACD registrados. "
                    "No se modifica OML:CALLS."
                )
                return

            redis_keys = cls.REDIS_DIALER_CONNECTION.keys('OML:CALLS:*:DIALER') or []

            camps_to_check = set(asterisk_counts.keys())
            camps_to_check.update(asterisk_ringing.keys())
            for key in redis_keys:
                try:
                    key_s = cls._decode_redis_str(key)
                    camps_to_check.add(int(key_s.split(':')[2]))
                except (IndexError, ValueError, TypeError):
                    continue

            corrections = 0
            for camp_id in camps_to_check:
                key = f'OML:CALLS:{camp_id}:DIALER'
                redis_raw = cls.REDIS_DIALER_CONNECTION.get(key)
                try:
                    redis_count = int(redis_raw or 0)
                except (TypeError, ValueError):
                    redis_count = 0
                observed = int(asterisk_counts.get(camp_id, 0))
                dialer_status = cls._get_campaign_dialer_status(camp_id)
                campaign_active = dialer_status == ACTIVE

                if redis_count == observed:
                    cls._clear_audit_streak(camp_id, kind='calls')
                    # CALLS ya alineado a 0: purgar phase keys huérfanas que
                    # el match temprano nunca pasaba por _align_phases.
                    if observed == 0 and campaign_active:
                        try:
                            _ph = list(cls.REDIS_DIALER_CONNECTION.scan_iter(
                                match=f'OML:CALLS:PHASE:{camp_id}:*',
                                count=50,
                            ))
                        except Exception:
                            _ph = []
                        if _ph:
                            logger.warning(
                                'Audit camp %s: CALLS=0 but %s orphan phase '
                                'keys; purging',
                                camp_id, len(_ph),
                            )
                            cls._align_phases_after_audit_down(camp_id, 0)
                elif redis_count < observed:
                    # Undercount → over-dial risk; allow partial cluster as lower bound
                    n = cls._bump_audit_streak(
                        camp_id, AUDIT_STREAK_DIR_UP, observed, kind='calls',
                    )
                    if n >= AUDIT_UP_STREAK_TICKS:
                        logger.warning(
                            'Audit corrected camp %s: redis=%s asterisk=%s '
                            'status=%s -> set %s (up streak=%s complete=%s)',
                            camp_id, redis_count, observed, dialer_status,
                            observed, n, cluster_complete,
                        )
                        cls.REDIS_DIALER_CONNECTION.set(key, observed)
                        cls._publish_calls_count(camp_id)
                        corrections += 1
                        redis_count = observed
                        cls._clear_audit_streak(camp_id, kind='calls')
                else:
                    # redis_count > observed (ghost / overcount)
                    can_down = False
                    down_needed = AUDIT_DOWN_STREAK_TICKS
                    safe_down_target = observed
                    # Señal fresh: observed puede ser 0 con nodos vivos; incompleto
                    # sin ningún nodo fresh ya retorna counts vacíos y se omite arriba
                    # vía NODES vacío / no fresh. Aquí incompleto ⇒ hay fresh>0.
                    if not campaign_active and redis_count > 0:
                        if cluster_complete:
                            can_down = True
                            down_needed = AUDIT_IDLE_DOWN_STREAK_TICKS
                        else:
                            # Cota inferior con streak más largo (nodo stale aún no
                            # expulsado).
                            can_down = True
                            down_needed = max(
                                AUDIT_IDLE_DOWN_STREAK_TICKS,
                                int(AUDIT_INCOMPLETE_DOWN_STREAK_TICKS),
                            )
                    elif campaign_active:
                        recent_reserves = (
                            cls._count_recent_reserves(camp_id)
                            if cls._campaign_has_recent_reserve(camp_id)
                            else 0
                        )
                        safe_down_target = observed + recent_reserves
                        if redis_count <= safe_down_target:
                            cls._clear_audit_streak(camp_id, kind='calls')
                            logger.debug(
                                'Audit: skip camp %s (recent reserve=%s) '
                                'redis=%s asterisk=%s safe=%s',
                                camp_id, recent_reserves, redis_count,
                                observed, safe_down_target,
                            )
                        elif cluster_complete:
                            can_down = True
                            down_needed = AUDIT_DOWN_STREAK_TICKS
                        else:
                            can_down = True
                            down_needed = max(
                                1, int(AUDIT_INCOMPLETE_DOWN_STREAK_TICKS),
                            )
                            logger.debug(
                                'Audit: incomplete-cluster down camp %s '
                                'redis=%s asterisk=%s need_streak=%s',
                                camp_id, redis_count, observed, down_needed,
                            )

                    if can_down:
                        n = cls._bump_audit_streak(
                            camp_id, AUDIT_STREAK_DIR_DOWN, safe_down_target,
                            kind='calls',
                        )
                        if n >= down_needed:
                            logger.warning(
                                'Audit corrected camp %s: redis=%s asterisk=%s '
                                'status=%s -> set %s (down streak=%s)',
                                camp_id, redis_count, observed, dialer_status,
                                safe_down_target, n,
                            )
                            cls.REDIS_DIALER_CONNECTION.set(key, safe_down_target)
                            cls._publish_calls_count(camp_id)
                            corrections += 1
                            redis_count = safe_down_target
                            cls._clear_audit_streak(camp_id, kind='calls')
                            cls._align_phases_after_audit_down(
                                camp_id, safe_down_target,
                            )

                cls._reconcile_campaign_ringing_bucket(
                    camp_id,
                    int(asterisk_ringing.get(camp_id, 0)),
                    redis_count,
                    cluster_complete,
                )

            logger.info(
                "Auditoría completada (%s correcciones, asterisk_camps=%s, "
                "cluster_complete=%s).",
                corrections, len(asterisk_counts), cluster_complete,
            )

        except Exception as e:
            logger.error(f"Error crítico en audit_active_channels: {e}", exc_info=True)

    @classmethod
    @job_handler_decorator
    def audit_active_channels_job(cls, worker, job):
        """
        Consumer Gearman de audit-active-channels.
        Usa lock Redis para evitar solapes entre réplicas/schedulers.
        """
        token = cls._acquire_audit_lock()
        if not token:
            logger.info(
                'audit-active-channels: lock ocupado, omitiendo ejecución solapada',
            )
            return b'Audit skipped: lock busy'
        try:
            cls.audit_active_channels()
            return b'Audit completed'
        finally:
            cls._release_audit_lock(token)

    @classmethod
    def handle_amd_option(cls, data):
        event = 'AMD'  # entidad propia: history (10, 1) y métricas "AMD Detected"
        id_campaign = data['id_campaign']
        id_contact = data['id_contact']
        phone_number = data['phone_number']
        logger.debug(f'Campaign {id_campaign}: receiving {event} for contact {id_contact}')
        cls.set_contact_status(id_campaign, id_contact, event)
        cls.handle_incidence_rules(event, id_campaign, id_contact, phone_number)
        return b'Disposition for AMD was handled'

    @classmethod
    @job_handler_decorator
    def add_incidence_rule_disposition(cls, worker, job):
        data = cls.decode_payload(job.data)
        disposition_option = data['disposition_option']
        if disposition_option == -2:
            return cls.handle_amd_option(data)
        return cls.handle_disposition_option(data)

    @classmethod
    @job_handler_decorator
    def create_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        status_custom = data['status_custom']
        max_attempt = data['max_attempt']
        retry_later = data['retry_later']
        mode = data['mode']
        STATUS = 1
        logger.debug(f'Adding incidence rule to campaign {id_campaign}')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if type_rule == STATUS:
                status = data['status']
                cursor_dialer.execute(
                    """INSERT INTO incidence_rules (id, status, status_custom, max_attempt,
                    retry_later, in_mode, campaign_id) VALUES
                    (%s, %s, %s, %s, %s, %s, %s);""",
                    (id_rule, status, status_custom, max_attempt, retry_later, mode, id_campaign))
            else:
                disposition_option_id = data['disposition_option_id']
                cursor_dialer.execute(
                    """INSERT INTO incidence_rules_disposition (id, disposition_option_id,
                    max_attempt, retry_later, in_mode, campaign_id) VALUES
                    (%s, %s, %s, %s, %s, %s);""",
                    (id_rule, disposition_option_id, max_attempt, retry_later, mode, id_campaign))
            return b'Incidence rule was added'

    @classmethod
    @job_handler_decorator
    def delete_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        type_rules = ['STATUS', 'DISPOSITION']
        type_rule_label = type_rules[type_rule - 1]
        STATUS = 1
        logger.debug(f'Removing incidence_rule {id_rule} of type {type_rule_label} '
                     f'in campaign with id = {id_campaign}')
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            if type_rule == STATUS:
                cursor.execute('DELETE FROM incidence_rules WHERE id = %s;', (id_rule,))
            else:
                cursor.execute('DELETE FROM incidence_rules_disposition WHERE id = %s;', (id_rule,))
            return b'Incidence rule was deleted'

    @classmethod
    @job_handler_decorator
    def update_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']  # 1 = STATUS, 2 = DISPOSITION

        # comunes
        max_attempt = data['max_attempt']
        retry_later = data['retry_later']
        mode = data['mode']

        STATUS = 1
        logger.debug(
            f'Updating incidence rule {id_rule} (type={type_rule}) in campaign {id_campaign}'
        )

        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()

            if type_rule == STATUS:
                status = data['status']
                status_custom = data['status_custom']
                cursor_dialer.execute(
                    """UPDATE incidence_rules
                    SET status = %s,
                        status_custom = %s,
                        max_attempt = %s,
                        retry_later = %s,
                        in_mode = %s,
                        campaign_id = %s
                    WHERE id = %s;""",
                    (status, status_custom, max_attempt, retry_later, mode, id_campaign, id_rule)
                )
            else:
                disposition_option_id = data['disposition_option_id']
                cursor_dialer.execute(
                    """UPDATE incidence_rules_disposition
                    SET disposition_option_id = %s,
                        max_attempt = %s,
                        retry_later = %s,
                        in_mode = %s,
                        campaign_id = %s
                    WHERE id = %s;""",
                    (disposition_option_id, max_attempt, retry_later, mode, id_campaign, id_rule)
                )

        try:
            cls.get_incidence_rule.cache_clear()
            cls.get_incidence_rule_disposition.cache_clear()
        except AttributeError:
            pass

        return b'Incidence rule was updated'

    @classmethod
    @job_handler_decorator
    def change_database(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        # 0- Pause the campaign
        cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=True)
        # 1- remove Redis related reports & contacts history
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{id_campaign}:COUNTER')
        for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                match=f'CONTACT:*:CAMP:{id_campaign}', count=1000):
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key)
        for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                match=f'CONTACT:*:CAMP:{id_campaign}:HISTORY', count=1000):
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key)
        # 2- remove Postgres related reports & contacts history
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'DELETE FROM contact_in_campaign WHERE id_campaign = %s', (id_campaign,))
                    cursor_dialer.execute(
                        'UPDATE campaign SET statistics = NULL WHERE id = %s', (id_campaign,))
                    # 3- bring the new contacts from OML
                    cls.copy_contacts_from_oml(cursor_dialer, cursor_oml, id_campaign)
        return b'Database was updated'

    @classmethod
    def collect_predictive_stats(cls, id_campaign):
        """
        Lee los hashes de métricas predictivas (Redis dialer DB3) para la
        vista HTMX admin. Error Redis en un hash: log + sección vacía
        (no debe romper el render del modal).
        """
        cls.connect_redis_dialer()
        sections = {}
        for section, key_template in PREDICTIVE_STATS_HASHES:
            key = key_template.format(id_campaign)
            try:
                sections[section] = \
                    cls.REDIS_DIALER_CONNECTION.hgetall(key) or {}
            except Exception:
                logger.exception('Error reading %s for HTMX stats', key)
                sections[section] = {}
        return sections

    @classmethod
    @job_handler_decorator
    def render_template(cls, worker, job):
        # Job dedicated to HTMX rendering
        data = cls.decode_payload(job.data)
        if data['type'] == 'init':
            logger.debug('HTMX related: getting the information of campaigns for the first time')
            with cls.get_dialer_connection() as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute('SELECT id, name, dialer_status from campaign;')
                campaigns = cursor_dialer.fetchall()
                # transforming dialer_status value to a label
                campaigns = [(id_camp, name, CAMPAIGN_STATUS_TO_NAME[status],
                              AVAILABLE_NEXT_STATUSES[status])
                             for (id_camp, name, status) in campaigns]
                cursor_dialer.execute('SELECT is_active FROM system_control;')
                running = cursor_dialer.fetchone()[0]
                return AdminRender.render_init(campaigns, running)
        if data['type'] == 'stats':
            id_campaign = data['id_campaign']
            cls.connect_redis_dialer()
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            return AdminRender.render_stats(
                id_campaign, stats,
                predictive=cls.collect_predictive_stats(id_campaign))
        if data['type'] == 'stats_inner':
            id_campaign = data['id_campaign']
            cls.connect_redis_dialer()
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            return AdminRender.render_stats_inner(id_campaign, stats)
        if data['type'] == 'pacing':
            id_campaign = data['id_campaign']
            return AdminRender.render_pacing(
                id_campaign, cls.collect_predictive_stats(id_campaign))

    @classmethod
    def stop_dialer(cls):
        logger.debug('Stopping dialer')
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'UPDATE system_control '
                        'SET is_active = false, updated_at = now() WHERE id = true;'
                    )
                    logger.debug('Pausing all active campaigns')
                    cursor_dialer.execute(
                        'UPDATE campaign '
                        'SET dialer_status = %s WHERE dialer_status = %s RETURNING id;',
                        (PAUSED, ACTIVE)
                    )
                    cursor_oml.execute(
                        'UPDATE ominicontacto_app_campana '
                        'SET estado = %s WHERE estado = %s;',
                        (PAUSED, ACTIVE)
                    )

        # **mantener Redis en sync** después del bulk
        cls.rebuild_active_campaigns_set()
        cls.connect_redis_oml()
        cls._notify_dialer_event(json.dumps({'type': 'PAUSE_BULK', 'camp_id': 'all'})
        )

    @classmethod
    def start_dialer(cls):
        logger.debug('Starting dialer')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'UPDATE system_control SET is_active = true, updated_at = now() WHERE id = true;')
        # mantener Redis en sync
        cls.rebuild_active_campaigns_set()
        cls.update_percentages_priority_campaigns()

    @classmethod
    def handle_dialer_action(cls, action):
        if action == 'start':
            cls.start_dialer()
        elif action == 'stop':
            cls.stop_dialer()
        else:
            cls.stop_dialer()
            cls.start_dialer()

    @classmethod
    @job_handler_decorator
    def manage_dialer(cls, worker, job):
        data = cls.decode_payload(job.data)
        action = data['action']
        cls.handle_dialer_action(action)
        cls.connect_redis_oml()
        cls._notify_dialer_event(json.dumps({'type': 'DIALER_STATUS_CHANGE',
                        'action': action,
                        'camp_id': 'all'})
        )
        running = True
        if action == "stop":
            running = False
        return AdminRender.render_status_dialer(running)


class SchedulerWorker(AverageWorker):
    """
    Worker del scheduler (naive datetimes, sin TZ/UTC).
    - Usa jobstore Redis (db configurable, por defecto 3).
    - Decrementa agendas de forma confiable e idempotente ante ADDED/EXECUTED/ERROR/REMOVED/MISSED.
    - Evita doble incremento cuando se reemplaza un job existente.
    """

    # ---------- Infra del scheduler ----------
    EXECUTORS = {'default': ThreadPoolExecutor(1)}
    JOB_DEFAULTS = {'misfire_grace_time': 3600, 'coalesce': True, 'max_instances': 1}
    JOBSTORES = {
        'default': RedisJobStore(
            host=REDIS_DIALER_SERVER,
            port=int(REDIS_DIALER_PORT),
            db=REDIS_DIALER_DB,
            jobs_key='apscheduler.jobs',
            run_times_key='apscheduler.run_times',
            # password=os.getenv('REDIS_DIALER_PASSWORD')
        )
    }
    SCHEDULER = BackgroundScheduler(
        executors=EXECUTORS,
        job_defaults=JOB_DEFAULTS,
        jobstores=JOBSTORES,
    )

    # ---------- Redis auxiliar (para "seen set" y fallback) ----------
    @classmethod
    def _redis(cls):
        return redis.StrictRedis(
            host=REDIS_DIALER_SERVER,
            port=int(REDIS_DIALER_PORT),
            db=REDIS_DIALER_DB,
            decode_responses=True,
        )

    @staticmethod
    def _camp_seen_key(camp_id: int) -> str:
        return f"CAMP:{camp_id}:SCHED:SEEN"

    # ---------- Leader lease (APScheduler 3 + RedisJobStore compartido) ----------
    @staticmethod
    def _scheduler_node_id() -> str:
        configured = os.environ.get('OML_NODE_ID', '').strip()
        if configured:
            return configured
        return socket.gethostname()

    @classmethod
    def _make_lease_token(cls) -> str:
        return f'{cls._scheduler_node_id()}:{uuid.uuid4().hex}'

    @classmethod
    def _lease_acquire(cls, token: str, ttl: int = SCHEDULER_LEADER_TTL_SEC) -> bool:
        try:
            return bool(cls._redis().set(
                SCHEDULER_LEADER_KEY, token, nx=True, ex=int(ttl),
            ))
        except Exception:
            logger.exception('Scheduler lease acquire failed key=%s', SCHEDULER_LEADER_KEY)
            return False

    @classmethod
    def _lease_renew(cls, token: str, ttl: int = SCHEDULER_LEADER_TTL_SEC) -> bool:
        try:
            return bool(cls._redis().eval(
                _LEASE_RENEW_LUA, 1, SCHEDULER_LEADER_KEY, token, int(ttl),
            ))
        except Exception:
            logger.exception('Scheduler lease renew failed key=%s', SCHEDULER_LEADER_KEY)
            return False

    @classmethod
    def _lease_release(cls, token: str) -> bool:
        try:
            return bool(cls._redis().eval(
                _LEASE_RELEASE_LUA, 1, SCHEDULER_LEADER_KEY, token,
            ))
        except Exception:
            logger.exception('Scheduler lease release failed key=%s', SCHEDULER_LEADER_KEY)
            return False

    @classmethod
    def _become_scheduler_leader(cls, token: str):
        global _SCHED_LEASE_TOKEN, _SCHED_IS_LEADER
        _SCHED_LEASE_TOKEN = token
        if not _SCHED_IS_LEADER:
            _SCHED_IS_LEADER = True
            try:
                if cls.SCHEDULER.running:
                    cls.SCHEDULER.resume()
            except Exception:
                logger.exception('Scheduler resume as leader failed')
            logger.info(
                'Scheduler leader acquired key=%s node=%s',
                SCHEDULER_LEADER_KEY, cls._scheduler_node_id(),
            )

    @classmethod
    def _relinquish_scheduler_leader(cls, reason: str):
        global _SCHED_LEASE_TOKEN, _SCHED_IS_LEADER
        was_leader = _SCHED_IS_LEADER
        _SCHED_LEASE_TOKEN = None
        _SCHED_IS_LEADER = False
        try:
            if cls.SCHEDULER.running:
                cls.SCHEDULER.pause()
        except Exception:
            logger.exception('Scheduler pause as follower failed')
        if was_leader:
            logger.info(
                'Scheduler leader lost (%s) key=%s node=%s',
                reason, SCHEDULER_LEADER_KEY, cls._scheduler_node_id(),
            )
        elif reason == 'held_by_peer':
            logger.debug(
                'Scheduler follower (lease held by peer) key=%s node=%s',
                SCHEDULER_LEADER_KEY, cls._scheduler_node_id(),
            )

    @classmethod
    def _leader_lease_tick(cls):
        """Intenta adquirir o renovar el lease; resume/pause según resultado."""
        global _SCHED_LEASE_TOKEN
        with _SCHED_LEASE_LOCK:
            token = _SCHED_LEASE_TOKEN
            if token:
                if cls._lease_renew(token):
                    cls._become_scheduler_leader(token)
                    return
                cls._relinquish_scheduler_leader('renew_failed')
            new_token = cls._make_lease_token()
            if cls._lease_acquire(new_token):
                cls._become_scheduler_leader(new_token)
            else:
                cls._relinquish_scheduler_leader('held_by_peer')

    @classmethod
    def _leader_lease_loop(cls):
        while not _SCHED_LEASE_STOP.is_set():
            try:
                cls._leader_lease_tick()
            except Exception:
                logger.exception('Scheduler leader lease tick failed')
            _SCHED_LEASE_STOP.wait(SCHEDULER_LEADER_RENEW_SEC)

    @classmethod
    def _start_leader_lease_once(cls):
        """Arranca el hilo daemon de lease una sola vez por proceso."""
        global _SCHED_LEASE_THREAD
        with _SCHED_LEASE_LOCK:
            if _SCHED_LEASE_THREAD is not None and _SCHED_LEASE_THREAD.is_alive():
                return
            _SCHED_LEASE_STOP.clear()
            thread = threading.Thread(
                target=cls._leader_lease_loop,
                name='scheduler-leader-lease',
                daemon=True,
            )
            _SCHED_LEASE_THREAD = thread
            thread.start()
            logger.info(
                'Scheduler leader lease thread started key=%s ttl=%ss renew=%ss',
                SCHEDULER_LEADER_KEY, SCHEDULER_LEADER_TTL_SEC, SCHEDULER_LEADER_RENEW_SEC,
            )

    @classmethod
    def _ensure_scheduler_started(cls):
        """
        Arranca APScheduler en paused y deja que el lease decida quién hace resume().

        Todos los nodos pueden add_job sobre el RedisJobStore; solo el líder ejecuta.
        """
        cls._register_listener_once()
        if not cls.SCHEDULER.running:
            cls.SCHEDULER.start(paused=True)
            logger.info('Scheduler started paused=True (awaiting leader lease)')
        cls._start_leader_lease_once()
        cls._register_audit_job_once()

    # ---------- Helpers de extracción ----------
    @classmethod
    def _extract_campaign_id_from_job_or_id(cls, job, job_id: str | None = None) -> int | None:
        """
        Extrae id_campaign desde:
        1) name/id del objeto job (name *_<id>_* ó *_<id>)
        2) args/kwargs del job
        3) fallback: patrón en job_id de APScheduler (p.ej. 'agenda_contact:{id}:{...}')
        """
        try:
            name = (getattr(job, 'name', '') or '') + '_' + (getattr(job, 'id', '') or '')
            m = re.search(r'_(\d+)(?:_|$)', name)
            if m:
                return int(m.group(1))

            # Fallback: args / kwargs
            args = getattr(job, 'args', ()) or ()
            for a in args:
                if isinstance(a, int):
                    return a
                if isinstance(a, str) and a.isdigit():
                    return int(a)

            kwargs = getattr(job, 'kwargs', {}) or {}
            for v in kwargs.values():
                if isinstance(v, int):
                    return v
                if isinstance(v, str) and v.isdigit():
                    return int(v)
            # 3) Fallback: parsear job_id del evento (agenda_contact:9:104 / process_campaign:9:...)
            if job_id:
                m2 = re.search(r':(\d+)(?::|$)', job_id)
                if m2:
                    return int(m2.group(1))
        except Exception as e:
            logger.debug("extract_campaign_id_from_job failed: %s", e)
        return None

    # ---------- Decremento seguro e idempotente ----------
    @classmethod
    def _agendas_decrement_once(cls, camp_id: int, job_id: str):
        """
        Decrementa una sola vez por job_id usando un set "seen".
        Si AverageWorker.agendas_decrement_safe existe, lo usa.
        Si no, hace SADD y luego agendas_decrement normal.
        """
        # Si el proyecto ya define un método seguro, úsalo
        if hasattr(AverageWorker, 'agendas_decrement_safe'):
            try:
                AverageWorker.agendas_decrement_safe(camp_id, job_id)
                return
            except Exception as e:
                logger.warning("agendas_decrement_safe falló, fallback a seen set: %s", e)

        # Fallback con seen set
        r = cls._redis()
        seen_key = cls._camp_seen_key(camp_id)
        try:
            added = r.sadd(seen_key, job_id)  # 1 si no existía
            if added == 1:
                r.expire(seen_key, AverageWorker.SEEN_TTL_SECONDS)
                AverageWorker.agendas_decrement(camp_id, 1)
        except Exception as e:
            logger.error("Fallo decremento seen-set para camp %s job %s: %s", camp_id, job_id, e)

    # ---------- Listener de eventos + logs ----------
    @staticmethod
    def _event_name(mask: int) -> str:
        if mask == EVENT_JOB_ADDED:
            return "ADDED"
        if mask == EVENT_JOB_REMOVED:
            return "REMOVED"
        if mask == EVENT_JOB_EXECUTED:
            return "EXECUTED"
        if mask == EVENT_JOB_ERROR:
            return "ERROR"
        if mask == EVENT_JOB_MISSED:
            return "MISSED"
        return f"UNKNOWN({mask})"

    @classmethod
    def _log_job_add(cls, *, job_id: str, name: str, run_date, exists_before: bool):
        try:
            jobstore_aliases = ",".join(cls.SCHEDULER._jobstores.keys())  # pragma: no cover
        except Exception:
            jobstore_aliases = "unknown"

        logger.info(
            "SCHED ADD %s | name=%s run_date=%s jobstores=%s replaced=%s",
            job_id, name, run_date, jobstore_aliases, exists_before,
        )

    @classmethod
    def _replacing_key(cls, job_id: str) -> str:
        return f"SCHED:REPLACING:{job_id}"

    @classmethod
    def _is_replacing(cls, job_id: str) -> bool:
        """True si el job_id está siendo reemplazado (replace_existing)."""
        try:
            return bool(cls._redis().get(cls._replacing_key(job_id)))
        except Exception:
            logger.debug("No se pudo leer replacing flag para %s", job_id, exc_info=True)
            return False

    @classmethod
    def _agenda_decrement_listener(cls, event):
        """Listener unificado: log + decremento idempotente (y safe ante replace_existing)."""
        try:
            jobstore = getattr(event, "jobstore", "unknown")
            evt = cls._event_name(event.code)

            # IMPORTANTE: en REMOVED el job puede ya no existir en el scheduler.
            job = None
            try:
                job = cls.SCHEDULER.get_job(event.job_id)
            except Exception:
                job = None

            camp_id = cls._extract_campaign_id_from_job_or_id(
                job, getattr(event, "job_id", None)
            )

            logger.info(
                "SCHED EVT %s | job_id=%s jobstore=%s campaign_id=%s",
                evt, event.job_id, jobstore, camp_id,
            )

            # (1) Si es REMOVED por replace_existing => NO decrementar.
            if event.code == EVENT_JOB_REMOVED:
                if cls._is_replacing(event.job_id):
                    logger.info(
                        "SCHED EVT REMOVED por replace_existing (skip decrement) job_id=%s",
                        event.job_id
                    )
                    return

            # (2) Decremento idempotente en eventos terminales
            if (
                event.code in (
                    EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_REMOVED,
                    EVENT_JOB_MISSED
                )
                and camp_id is not None
            ):
                cls._agendas_decrement_once(camp_id, event.job_id)

        except Exception as e:
            logger.warning("Agenda decrement/log listener failed: %s", e, exc_info=True)

    @classmethod
    def _register_listener_once(cls):
        """Registra el listener una sola vez (alta/baja/exec/error/missed)."""
        global _SCHED_LISTENER_REGISTERED
        if _SCHED_LISTENER_REGISTERED:
            return
        cls.SCHEDULER.add_listener(
            cls._agenda_decrement_listener,
            EVENT_JOB_ADDED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR |
            EVENT_JOB_REMOVED | EVENT_JOB_MISSED,
        )
        _SCHED_LISTENER_REGISTERED = True
        logger.debug("Scheduler listener registrado para ADDED/EXECUTED/ERROR/REMOVED/MISSED")

    @classmethod
    def _enqueue_audit_active_channels(cls):
        """Productor liviano: encola job Gearman audit-active-channels."""
        try:
            client = cls._get_gearman_client()
            client.submit_job(
                AUDIT_ACTIVE_CHANNELS_JOB,
                b'{}',
                background=True,
            )
            logger.debug('Enqueued %s', AUDIT_ACTIVE_CHANNELS_JOB)
        except Exception as e:
            logger.error(
                'Failed to enqueue %s: %s', AUDIT_ACTIVE_CHANNELS_JOB, e, exc_info=True,
            )

    @classmethod
    def _register_audit_job_once(cls):
        """Programa encolado periódico de audit-active-channels (no ejecuta la reconciliación)."""
        global _AUDIT_JOB_REGISTERED
        if _AUDIT_JOB_REGISTERED:
            return
        interval = CHANNEL_AUDIT_INTERVAL_SEC
        if interval <= 0:
            logger.info("CHANNEL_AUDIT_INTERVAL_SEC=%s: audit job deshabilitado", interval)
            _AUDIT_JOB_REGISTERED = True
            return
        job_id = 'audit_dialer_channels'
        cls.SCHEDULER.add_job(
            cls._enqueue_audit_active_channels,
            'interval',
            seconds=interval,
            id=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logger.info(
            "Scheduler: enqueue %s cada %ss (job_id=%s)",
            AUDIT_ACTIVE_CHANNELS_JOB, interval, job_id,
        )
        _AUDIT_JOB_REGISTERED = True

    @classmethod
    def start_periodic_jobs(cls):
        """
        Inicializa APScheduler y jobs periódicos al arrancar el worker scheduler.
        Independiente de schedule-agenda. Solo el líder del lease ejecuta jobs.
        """
        cls._ensure_scheduler_started()

    # ---------- Funciones ejecutadas por los jobs ----------
    @classmethod
    def schedule_contact(cls, phone_number, id_campaign, id_contact):
        logger.debug(
            f'Campaign {id_campaign}: firing scheduled agenda for contact {id_contact}'
        )
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)
            if status_campaign != ACTIVE:
                if id_contact:
                    cursor_dialer.execute(
                        'UPDATE contact_in_campaign SET schedule_aborted = true '
                        'WHERE id_contact = %s AND id_campaign = %s;',
                        (id_contact, id_campaign),
                    )
                logger.info(
                    'Campaign %s: schedule_contact skipped (status=%s) contact=%s',
                    id_campaign, status_campaign, id_contact,
                )
                return 'Skipped: campaign not active'

        reserve_reason = cls._reserve_dialer_channel(id_campaign, id_contact)
        if reserve_reason != RESERVE_OK:
            # Con phase viva no reencolar: la llamada ya está en curso.
            if reserve_reason != RESERVE_PHASE_EXISTS:
                cls._mark_contact_status_created(id_campaign, id_contact)
            if reserve_reason == RESERVE_PHASE_EXISTS:
                return 'Skipped: contact already in flight'
            return 'Skipped: no free dialer channels'

        dial_attrs = None
        if ATTEMPT_ID_ENABLED:
            cls.connect_redis_dialer()
            aid = get_attempt_id(
                cls.REDIS_DIALER_CONNECTION, id_campaign, id_contact,
            )
            if aid:
                dial_attrs = {'attempt_id': aid}
        success = cls.trigger_acd_dial(
            phone_number=phone_number,
            campaign_id=id_campaign,
            contact_id=id_contact,
            attributes=dial_attrs,
        )
        if success:
            return 'GD!!!'

        cls._decrement_calls_once(
            id_campaign, id_contact, None,
            context='schedule_contact_gearman_rollback', use_dedup=True,
        )
        cls._mark_contact_status_created(id_campaign, id_contact)
        logger.warning(
            f'Campaign {id_campaign}: Failed to send dial job for contact {id_contact}'
        )
        return 'Error sending dial job'

    @classmethod
    def schedule_process_campaign(cls, id_campaign):
        logger.debug(f'Campaign {id_campaign} starting to run from the scheduler')
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        return 'GD!!!'

    # ---------- API de agenda (entrypoint Gearman) ----------
    @classmethod
    @job_handler_decorator
    def schedule_agenda(cls, worker, job):
        cls._ensure_scheduler_started()

        data = cls.decode_payload(job.data)
        id_campaign = int(data['id_campaign'])
        schedule_type = data.get('type', 'agenda')

        # Parser naive (sin TZ)
        def _parse_naive(dt_str: str) -> datetime.datetime:
            return datetime.datetime.strptime(dt_str, '%d/%m/%y %H:%M:%S')

        # Helper: limpiar seen siempre (corrige punto 2)
        def _clear_seen(job_id: str):
            try:
                cls._redis().srem(cls._camp_seen_key(id_campaign), job_id)
            except Exception as e:
                logger.warning("No se pudo limpiar seen set para %s: %s", job_id, e)

        # Helper: marcar reemplazo SIEMPRE (robusto ante falsos negativos de exists)
        def _mark_replacing(job_id: str):
            try:
                # TTL corto: cubre la ventana de REMOVED->ADDED durante replace_existing
                cls._redis().setex(cls._replacing_key(job_id), 5, "1")
            except Exception:
                logger.debug("No se pudo setear replacing flag para %s", job_id, exc_info=True)

        if schedule_type == 'process-campaign':
            datetime_start_str = data.get('datetime_start', '')
            if not datetime_start_str:
                return b'Missing datetime_start'

            run_date = _parse_naive(datetime_start_str)

            # ID único determinístico: permite reprogramar sin doble incrementar
            job_id = f'process_campaign:{id_campaign}:{run_date.strftime("%Y%m%d%H%M%S")}'
            name = f'scheduled_process_campaign_{id_campaign}'

            # exists se usa SOLO para decidir incremento; no para replacing
            exists = cls.SCHEDULER.get_job(job_id) is not None

            # replace_existing=True => marcamos replacing siempre
            _mark_replacing(job_id)

            cls.SCHEDULER.add_job(
                cls.schedule_process_campaign,
                trigger='date',
                run_date=run_date,
                args=[id_campaign],
                id=job_id,
                name=name,
                replace_existing=True,
                misfire_grace_time=3600,
                coalesce=True,
                max_instances=1,
            )

            cls._log_job_add(job_id=job_id, name=name, run_date=run_date, exists_before=exists)

            # Limpiar seen siempre al agendar (exista o no)
            _clear_seen(job_id)

            # Incremento sólo si no existía (no duplicar agendas)
            if not exists:
                AverageWorker.agendas_increment(id_campaign, 1)

            return b'Campaign process was scheduled'

        # Agenda de contacto
        datetime_agenda_str = data.get('datetime_agenda', '')
        if not datetime_agenda_str:
            return b'Missing datetime_agenda'

        run_date = _parse_naive(datetime_agenda_str)
        id_contact = data.get('id_contact')
        phone_number = data.get('phone_number', '')
        if not id_contact:
            job_id = (
                f'agenda_contact:{id_campaign}:{phone_number}:'
                f'{run_date.strftime("%Y%m%d%H%M%S")}'
            )
        else:
            job_id = f'agenda_contact:{id_campaign}:{id_contact}'

        name = f'agenda_contact_{id_campaign}_{id_contact}'

        exists = cls.SCHEDULER.get_job(job_id) is not None

        # replace_existing=True => marcamos replacing siempre
        _mark_replacing(job_id)

        cls.SCHEDULER.add_job(
            cls.schedule_contact,
            trigger='date',
            run_date=run_date,
            args=[phone_number, id_campaign, id_contact],
            id=job_id,
            name=name,
            replace_existing=True,
            misfire_grace_time=3600,
            coalesce=True,
            max_instances=1,
        )

        cls._log_job_add(job_id=job_id, name=name, run_date=run_date, exists_before=exists)

        _clear_seen(job_id)

        if not exists:
            AverageWorker.agendas_increment(id_campaign, 1)

        return b'Agenda was scheduled'
