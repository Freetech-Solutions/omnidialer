"""
Redis Streams helper for dialer → supervision/omnidialer listeners (OML Redis DB 2).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

STREAM_DIALER = 'OML:STREAM:DIALER'

DEFAULT_EVENT_STREAM_MAXLEN = 10000
EVENT_STREAM_DB = 2

_stream_client = None


def event_stream_maxlen() -> int:
    try:
        return int(os.getenv('OML_EVENT_STREAM_MAXLEN', DEFAULT_EVENT_STREAM_MAXLEN))
    except (TypeError, ValueError):
        return DEFAULT_EVENT_STREAM_MAXLEN


def get_stream_redis():
    """Dedicated REDIS_OML connection on DB 2 for event streams."""
    global _stream_client
    if _stream_client is not None:
        return _stream_client
    import redis
    host = os.getenv('REDIS_OML_SERVER', 'oml-redis')
    port = int(os.getenv('REDIS_OML_PORT', '6379'))
    try:
        _stream_client = redis.Redis(
            host=host,
            port=port,
            db=EVENT_STREAM_DB,
            decode_responses=True,
        )
        _stream_client.ping()
        return _stream_client
    except Exception:
        logger.exception(
            'event_stream: failed to connect REDIS_OML db=%s %s:%s',
            EVENT_STREAM_DB, host, port,
        )
        _stream_client = None
        return None


def xadd_event(
    redis_conn,
    stream_name: str,
    payload: Dict[str, Any],
    maxlen: Optional[int] = None,
) -> bool:
    if redis_conn is None:
        return False
    if maxlen is None:
        maxlen = event_stream_maxlen()
    try:
        content = json.dumps(payload) if not isinstance(payload, str) else payload
        redis_conn.execute_command(
            'XADD', stream_name, 'MAXLEN', '~', int(maxlen), '*', 'value', content,
        )
        return True
    except Exception:
        logger.exception('event_stream.xadd_event failed stream=%s', stream_name)
        return False


def notify_dialer_event(payload: Dict[str, Any], publish_conn=None) -> None:
    """XADD dialer event to OML:STREAM:DIALER (DB 2). publish_conn ignored (cutover)."""
    stream_conn = get_stream_redis()
    xadd_event(stream_conn, STREAM_DIALER, payload)
