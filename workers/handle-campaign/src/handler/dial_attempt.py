"""
M0 DialAttempt: identidad de intento dialer-owned (aditivo).

Genera/propaga attempt_id sin cambiar el modelo de cupo
(OML:CALLS sigue keyed por contact_id / callid / fases).
"""
from __future__ import annotations

import uuid
from typing import Any, Optional


def new_attempt_id() -> str:
    """ULID-like opaco: UUIDv4 stdlib (sin deps extra)."""
    return str(uuid.uuid4())


def attempt_redis_key(id_campaign: Any, contact_id: Any) -> str:
    return f'OML:ATTEMPT:{id_campaign}:{contact_id}'


def store_attempt_id(
        redis_conn,
        id_campaign: Any,
        contact_id: Any,
        attempt_id: str,
        ttl_sec: int,
) -> None:
    """SET attempt_id con TTL alineado a la phase del canal."""
    if redis_conn is None or not attempt_id:
        return
    key = attempt_redis_key(id_campaign, contact_id)
    ttl = max(1, int(ttl_sec or 1))
    redis_conn.set(key, attempt_id, ex=ttl)


def get_attempt_id(redis_conn, id_campaign: Any, contact_id: Any) -> Optional[str]:
    """Lee attempt_id vivo; None si no hay key o Redis falla."""
    if redis_conn is None:
        return None
    try:
        raw = redis_conn.get(attempt_redis_key(id_campaign, contact_id))
    except Exception:
        return None
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode('utf-8', errors='replace')
    raw = str(raw).strip()
    return raw or None
