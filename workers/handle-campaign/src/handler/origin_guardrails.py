# -*- coding: utf-8 -*-
"""P0 origin guardrails: pause originates when event backlog or counter drift is high."""

from __future__ import annotations

import logging
import socket
import time
from typing import Dict, Optional, Tuple

from settings.default import (
    GEARMAN_JOB_SERVERS,
    GLOBAL_CAPS,
    GUARDRAIL_CALLS_AST_DELTA_MAX,
    GUARDRAIL_GEARMAN_STATUS_TTL_SEC,
    GUARDRAIL_PHASE_CALLS_DELTA_MAX,
    GUARDRAIL_PROCESS_EVENT_QUEUED_MAX,
    ORIGIN_GUARDRAILS_ENABLED,
)

logger = logging.getLogger(__name__)

_STATUS_CACHE: Dict[str, object] = {'ts': 0.0, 'queued': None}
GLOBAL_CAPS_KEY_PREFIX = 'OML:DIALER:GLOBAL_CAPS:'


def _gearman_host_port() -> Optional[Tuple[str, int]]:
    if not GEARMAN_JOB_SERVERS:
        return None
    raw = (GEARMAN_JOB_SERVERS[0] or '').strip()
    if not raw:
        return None
    if ':' in raw:
        host, _, port_s = raw.rpartition(':')
        try:
            return host.strip(), int(port_s)
        except (TypeError, ValueError):
            return host.strip(), 4730
    return raw, 4730


def fetch_process_event_queued(force: bool = False) -> Optional[int]:
    """
    Lee queued de process-event vía admin protocol Gearman (`status\\n`).
    Cache corto para no abrir socket en cada originate.
    None = no se pudo medir (fail-open).
    """
    ttl = max(0.1, float(GUARDRAIL_GEARMAN_STATUS_TTL_SEC or 1.0))
    now = time.time()
    if (
        not force
        and _STATUS_CACHE.get('queued') is not None
        and (now - float(_STATUS_CACHE.get('ts') or 0.0)) < ttl
    ):
        try:
            return int(_STATUS_CACHE['queued'])  # type: ignore[arg-type]
        except (TypeError, ValueError):
            pass

    endpoint = _gearman_host_port()
    if not endpoint:
        return None
    host, port = endpoint
    try:
        with socket.create_connection((host, port), timeout=1.5) as sock:
            sock.sendall(b'status\n')
            chunks = []
            sock.settimeout(1.5)
            while True:
                try:
                    data = sock.recv(8192)
                except socket.timeout:
                    break
                if not data:
                    break
                chunks.append(data)
                if b'.\n' in data or len(b''.join(chunks)) > 65536:
                    break
            raw = b''.join(chunks).decode('utf-8', errors='replace')
    except OSError as exc:
        logger.debug('origin_guardrails: gearman status failed host=%s: %s', host, exc)
        return None

    queued = None
    for line in raw.splitlines():
        if not line.strip() or '\t' not in line:
            continue
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        if parts[0].strip() != 'process-event':
            continue
        try:
            queued = int(parts[1])
        except (TypeError, ValueError):
            queued = None
        break

    _STATUS_CACHE['ts'] = now
    _STATUS_CACHE['queued'] = queued
    return queued


def try_consume_global_caps(redis_conn) -> Tuple[bool, str]:
    """
    Techo global originates/s (Redis INCR por segundo UNIX).
    GLOBAL_CAPS<=0 o guardrails off → siempre OK.
    """
    if not ORIGIN_GUARDRAILS_ENABLED:
        return True, ''
    try:
        limit = int(GLOBAL_CAPS or 0)
    except (TypeError, ValueError):
        limit = 0
    if limit <= 0:
        return True, ''
    if redis_conn is None:
        return True, ''
    try:
        key = f'{GLOBAL_CAPS_KEY_PREFIX}{int(time.time())}'
        n = int(redis_conn.incr(key))
        if n == 1:
            redis_conn.expire(key, 2)
        if n > limit:
            return False, 'global_caps'
        return True, ''
    except Exception:
        logger.debug('origin_guardrails: global CAPS incr failed', exc_info=True)
        return True, ''


def evaluate_backpressure_guardrails(
    *,
    active_calls: int,
    phase_total: int,
    ast_channels: Optional[int],
) -> Tuple[bool, str]:
    """
    Backpressure only (no global CAPS consume). Returns (should_block, reason).
    Fail-open when Gearman/Ast probes fail.
    """
    if not ORIGIN_GUARDRAILS_ENABLED:
        return False, ''

    try:
        qmax = int(GUARDRAIL_PROCESS_EVENT_QUEUED_MAX or 0)
    except (TypeError, ValueError):
        qmax = 0
    if qmax > 0:
        queued = fetch_process_event_queued()
        if queued is not None and queued >= qmax:
            return True, 'process_event_backlog'

    try:
        delta_max = int(GUARDRAIL_CALLS_AST_DELTA_MAX or 0)
    except (TypeError, ValueError):
        delta_max = 0
    if delta_max > 0 and ast_channels is not None:
        try:
            delta = int(active_calls) - int(ast_channels)
        except (TypeError, ValueError):
            delta = 0
        if delta > delta_max:
            return True, 'calls_ast_delta'

    try:
        phase_max = int(GUARDRAIL_PHASE_CALLS_DELTA_MAX or 0)
    except (TypeError, ValueError):
        phase_max = 0
    if phase_max > 0:
        try:
            phase_delta = int(phase_total) - int(active_calls)
        except (TypeError, ValueError):
            phase_delta = 0
        if phase_delta > phase_max:
            return True, 'phase_calls_delta'

    return False, ''


def check_endpoints_or_raise(redis_factory, require: bool) -> None:
    """
    Boot check: Redis ping + Gearman TCP. Logs error; raises if require=True.
    """
    errors = []
    try:
        r = redis_factory()
        if r is None or not r.ping():
            errors.append('redis_dialer_ping_failed')
    except Exception as exc:
        errors.append(f'redis_dialer:{exc}')

    endpoint = _gearman_host_port()
    if not endpoint:
        errors.append('gearman_unconfigured')
    else:
        host, port = endpoint
        try:
            with socket.create_connection((host, port), timeout=2.0):
                pass
        except OSError as exc:
            errors.append(f'gearman:{host}:{port}:{exc}')

    if not errors:
        logger.info(
            'origin_guardrails: endpoints OK gearman=%s',
            endpoint,
        )
        return

    msg = 'origin_guardrails: endpoint check failed: %s' % ('; '.join(errors),)
    if require:
        logger.error(msg)
        raise RuntimeError(msg)
    logger.warning(msg)
