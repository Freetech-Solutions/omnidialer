"""
Exporter Prometheus de pacing OMniDialer (lee Redis DB3).

Expone /metrics con gauges alineados al snapshot CAMP:{id}:PACING /
CHANNELS / COUNTER / OML:CALLS — una sola réplica, sin Gearman.
"""
from __future__ import annotations

import logging
import os
import re
import signal
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set

import redis

try:
    from prometheus_client import start_http_server, REGISTRY
    from prometheus_client.core import GaugeMetricFamily
    from prometheus_client.registry import Collector
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

    class Collector:  # type: ignore[no-redef]
        pass

    class GaugeMetricFamily:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            self.samples = []

        def add_metric(self, *args, **kwargs):
            pass

    def start_http_server(*args, **kwargs):
        pass

    REGISTRY = None

logger = logging.getLogger('omnidialer_metrics')

REDIS_DIALER_SERVER = os.getenv('REDIS_DIALER_SERVER', 'redis')
REDIS_DIALER_PORT = int(os.getenv('REDIS_DIALER_PORT', '6379'))
REDIS_DIALER_DB = int(os.getenv('REDIS_DIALER_DB', '3'))
METRICS_PORT = int(os.getenv('DIALER_METRICS_PORT', '7099'))
METRICS_ADDR = os.getenv('DIALER_METRICS_ADDR', '0.0.0.0')

_CAMP_PACING_RE = re.compile(r'^CAMP:(\d+):PACING$')
_COUNTER_NAMES = (
    'ATTEMPTED_CALLS',
    'ANSWERED_PSTN',
    'ANSWERED_AGENT',
    'AMD',
    'CHANUNAVAIL',
    'CONGESTION',
    'EXIT_ABANDON',
    'EXIT_SHORTCALL',
    'EXIT_TIMEOUT',
    'ORIGINATE_FAILED',
    'CONTACTED SUCCESSFULLY',
    'FINALIZED WITH NO CONTACT',
)
_PHASES = ('RINGING', 'WAITING_AGENT', 'ONCALL')


def _f(value: Any, default: float = 0.0) -> float:
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    return int(_f(value, float(default)))


class DialerPacingCollector(Collector):
    """Custom Prometheus collector: snapshots Redis DB3 on each scrape."""

    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client
        self.last_scrape_ok = 1.0
        self.last_refresh_ts = 0.0

    def discover_campaign_ids(self) -> List[str]:
        ids: Set[str] = set()
        for key in self._redis.scan_iter(match='CAMP:*:PACING', count=200):
            m = _CAMP_PACING_RE.match(str(key))
            if m:
                ids.add(m.group(1))
        for key in self._redis.scan_iter(match='OML:CALLS:*:DIALER', count=200):
            parts = str(key).split(':')
            if len(parts) >= 3 and parts[2].isdigit():
                ids.add(parts[2])
        # ACTIVE set: camp sin PACING (stall post-restart) sigue visible
        try:
            for raw in self._redis.smembers('campaigns:active') or []:
                cid = str(raw)
                if cid.isdigit():
                    ids.add(cid)
        except Exception:
            logger.debug('campaigns:active unavailable', exc_info=True)
        return sorted(ids, key=int)

    def collect_campaign(self, campaign_id: str) -> Dict[str, Any]:
        pacing = self._redis.hgetall(f'CAMP:{campaign_id}:PACING') or {}
        metrics = self._redis.hgetall(f'CAMP:{campaign_id}:METRICS') or {}
        channels = self._redis.hgetall(f'CAMP:{campaign_id}:CHANNELS') or {}
        counter = self._redis.hgetall(f'CAMP:{campaign_id}:COUNTER') or {}
        calls_raw = self._redis.get(f'OML:CALLS:{campaign_id}:DIALER')
        now = time.time()
        ts = _f(pacing.get('TS'), 0.0)
        age = (now - ts) if pacing and ts > 0 else -1.0
        p_hit = pacing.get('P_HIT')
        if p_hit in (None, ''):
            p_hit = metrics.get('P_HIT')
        drop = pacing.get('DROP_RATE')
        if drop in (None, ''):
            drop = metrics.get('DROP_RATE_EWMA') or metrics.get('DROP_RATE')
        return {
            'pacing_up': 1 if pacing else 0,
            'calls': _i(calls_raw, 0),
            'c_dial': _f(pacing.get('C_DIAL')),
            'gamma': _f(pacing.get('GAMMA')),
            'p_hit': _f(p_hit),
            'drop_ewma': _f(drop),
            'a_free': _f(pacing.get('A_FREE')),
            'throttle_latched': _i(pacing.get('THROTTLE_LATCHED'), 0),
            'mode': str(pacing.get('MODE') or '-'),
            'age': age,
            'channels': {ph: _i(channels.get(ph), 0) for ph in _PHASES},
            'counter': {
                name: _f(counter.get(name), 0.0) for name in _COUNTER_NAMES
            },
        }

    def collect(self):
        scrape_ok = GaugeMetricFamily(
            'omnidialer_metrics_scrape_ok',
            '1 if last Redis snapshot succeeded',
        )
        scrape_ts = GaugeMetricFamily(
            'omnidialer_metrics_last_refresh_timestamp',
            'Unix timestamp of last successful Redis snapshot',
        )
        calls = GaugeMetricFamily(
            'omnidialer_campaign_calls',
            'OML:CALLS dialer in-flight channels',
            labels=['campaign_id'],
        )
        channels = GaugeMetricFamily(
            'omnidialer_campaign_channels',
            'Channel phase counts from CAMP:{id}:CHANNELS',
            labels=['campaign_id', 'phase'],
        )
        c_dial = GaugeMetricFamily(
            'omnidialer_campaign_c_dial',
            'Predictive C_DIAL from last PACING snapshot',
            labels=['campaign_id'],
        )
        gamma = GaugeMetricFamily(
            'omnidialer_campaign_gamma',
            'Predictive GAMMA from PACING',
            labels=['campaign_id'],
        )
        p_hit = GaugeMetricFamily(
            'omnidialer_campaign_p_hit',
            'P_HIT from PACING (fallback METRICS)',
            labels=['campaign_id'],
        )
        drop_ewma = GaugeMetricFamily(
            'omnidialer_campaign_drop_rate_ewma',
            'Drop rate EWMA from PACING/METRICS',
            labels=['campaign_id'],
        )
        a_free = GaugeMetricFamily(
            'omnidialer_campaign_a_free',
            'A_FREE agents from PACING',
            labels=['campaign_id'],
        )
        throttle = GaugeMetricFamily(
            'omnidialer_campaign_throttle_latched',
            '1 if predictive throttle latched',
            labels=['campaign_id'],
        )
        pacing_up = GaugeMetricFamily(
            'omnidialer_campaign_pacing_up',
            '1 if CAMP:{id}:PACING key exists (live snapshot)',
            labels=['campaign_id'],
        )
        pacing_age = GaugeMetricFamily(
            'omnidialer_campaign_pacing_snapshot_age_seconds',
            'Seconds since PACING.TS (empty PACING → -1)',
            labels=['campaign_id'],
        )
        mode = GaugeMetricFamily(
            'omnidialer_campaign_mode',
            '1 for current PACING.MODE label',
            labels=['campaign_id', 'mode'],
        )
        counter = GaugeMetricFamily(
            'omnidialer_campaign_counter',
            'Session counters from CAMP:{id}:COUNTER',
            labels=['campaign_id', 'name'],
        )

        try:
            campaign_ids = self.discover_campaign_ids()
            for cid in campaign_ids:
                data = self.collect_campaign(cid)
                calls.add_metric([cid], data['calls'])
                c_dial.add_metric([cid], data['c_dial'])
                gamma.add_metric([cid], data['gamma'])
                p_hit.add_metric([cid], data['p_hit'])
                drop_ewma.add_metric([cid], data['drop_ewma'])
                a_free.add_metric([cid], data['a_free'])
                throttle.add_metric([cid], data['throttle_latched'])
                pacing_up.add_metric([cid], data['pacing_up'])
                pacing_age.add_metric([cid], data['age'])
                mode.add_metric([cid, data['mode']], 1)
                for ph, val in data['channels'].items():
                    channels.add_metric([cid, ph], val)
                for name, val in data['counter'].items():
                    counter.add_metric([cid, name], val)
            self.last_scrape_ok = 1.0
            self.last_refresh_ts = time.time()
        except Exception:
            logger.exception('Redis snapshot failed')
            self.last_scrape_ok = 0.0

        scrape_ok.add_metric([], self.last_scrape_ok)
        scrape_ts.add_metric([], self.last_refresh_ts)

        yield scrape_ok
        yield scrape_ts
        yield calls
        yield channels
        yield c_dial
        yield gamma
        yield p_hit
        yield drop_ewma
        yield a_free
        yield throttle
        yield pacing_up
        yield pacing_age
        yield mode
        yield counter


def connect_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_DIALER_SERVER,
        port=REDIS_DIALER_PORT,
        db=REDIS_DIALER_DB,
        decode_responses=True,
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    logging.basicConfig(
        level=os.getenv('PYTHON_LOGLEVEL', 'INFO'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    if not PROMETHEUS_AVAILABLE:
        logger.error('prometheus_client is required')
        return 1

    client = connect_redis()
    try:
        client.ping()
    except Exception:
        logger.exception(
            'Cannot reach Redis %s:%s db=%s',
            REDIS_DIALER_SERVER, REDIS_DIALER_PORT, REDIS_DIALER_DB,
        )
        return 1

    collector = DialerPacingCollector(client)
    REGISTRY.register(collector)
    start_http_server(METRICS_PORT, addr=METRICS_ADDR)
    logger.info(
        'Prometheus metrics listening on %s:%s (redis=%s:%s/%s)',
        METRICS_ADDR, METRICS_PORT,
        REDIS_DIALER_SERVER, REDIS_DIALER_PORT, REDIS_DIALER_DB,
    )

    stop = {'flag': False}

    def _shutdown(signum, frame):
        logger.info('signal %s received, stopping', signum)
        stop['flag'] = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    while not stop['flag']:
        time.sleep(1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
