"""Unit tests for omnidialer_metrics Redis → Prometheus collector."""
from __future__ import annotations

import fnmatch
import time
import unittest
from unittest.mock import MagicMock

import omnidialer_metrics as metrics


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.sets = {}

    def scan_iter(self, match=None, count=None):
        keys = list(self.hashes.keys()) + list(self.kv.keys())
        for k in keys:
            if match is None or fnmatch.fnmatch(k, match):
                yield k

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def get(self, key):
        return self.kv.get(key)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(str(m) for m in members)

    def hset(self, key, mapping=None, **kwargs):
        self.hashes.setdefault(key, {})
        if mapping:
            self.hashes[key].update({str(k): str(v) for k, v in mapping.items()})

    def set(self, key, value):
        self.kv[key] = str(value)


def _samples(collector):
    out = {}
    for metric in collector.collect():
        for s in metric.samples:
            out[(s.name, tuple(sorted(s.labels.items())))] = s.value
    return out


@unittest.skipUnless(metrics.PROMETHEUS_AVAILABLE, 'prometheus_client required')
class DialerPacingCollectorTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.collector = metrics.DialerPacingCollector(self.redis)

    def test_discover_from_pacing_and_calls(self):
        self.redis.hset('CAMP:19:PACING', mapping={'MODE': 'PREDICTIVE', 'TS': '1'})
        self.redis.set('OML:CALLS:7:DIALER', '2')
        self.redis.sadd('campaigns:active', '19', '42')
        ids = self.collector.discover_campaign_ids()
        self.assertEqual(ids, ['7', '19', '42'])

    def test_active_without_pacing_emits_pacing_up_zero(self):
        self.redis.sadd('campaigns:active', '19')
        samples = _samples(self.collector)
        self.assertEqual(
            samples.get(('omnidialer_campaign_pacing_up', (('campaign_id', '19'),))),
            0.0,
        )
        self.assertEqual(
            samples.get((
                'omnidialer_campaign_pacing_snapshot_age_seconds',
                (('campaign_id', '19'),),
            )),
            -1.0,
        )

    def test_collect_campaign_with_pacing(self):
        now = int(time.time())
        self.redis.hset(
            'CAMP:19:PACING',
            mapping={
                'MODE': 'PREDICTIVE',
                'C_DIAL': '3',
                'GAMMA': '1.5',
                'P_HIT': '0.4',
                'DROP_RATE': '0.01',
                'A_FREE': '2',
                'THROTTLE_LATCHED': '0',
                'TS': str(now - 5),
            },
        )
        self.redis.hset(
            'CAMP:19:CHANNELS',
            mapping={'RINGING': '1', 'WAITING_AGENT': '0', 'ONCALL': '2'},
        )
        self.redis.hset(
            'CAMP:19:COUNTER',
            mapping={'ATTEMPTED_CALLS': '10', 'AMD': '4', 'ANSWERED_AGENT': '3'},
        )
        self.redis.set('OML:CALLS:19:DIALER', '3')
        data = self.collector.collect_campaign('19')
        self.assertEqual(data['pacing_up'], 1)
        self.assertEqual(data['calls'], 3)
        self.assertEqual(data['c_dial'], 3.0)
        self.assertEqual(data['gamma'], 1.5)
        self.assertEqual(data['mode'], 'PREDICTIVE')
        self.assertEqual(data['channels']['RINGING'], 1)
        self.assertEqual(data['channels']['ONCALL'], 2)
        self.assertEqual(data['counter']['AMD'], 4.0)
        self.assertGreaterEqual(data['age'], 4.0)

    def test_collect_campaign_without_pacing(self):
        self.redis.set('OML:CALLS:19:DIALER', '0')
        data = self.collector.collect_campaign('19')
        self.assertEqual(data['pacing_up'], 0)
        self.assertEqual(data['mode'], '-')
        self.assertEqual(data['age'], -1.0)

    def test_prometheus_collect_emits_gauges(self):
        now = int(time.time())
        self.redis.hset(
            'CAMP:19:PACING',
            mapping={
                'MODE': 'PREDICTIVE',
                'C_DIAL': '1',
                'GAMMA': '1',
                'P_HIT': '0.5',
                'DROP_RATE': '0',
                'A_FREE': '1',
                'THROTTLE_LATCHED': '0',
                'TS': str(now),
            },
        )
        self.redis.set('OML:CALLS:19:DIALER', '1')
        samples = _samples(self.collector)
        self.assertEqual(
            samples.get(('omnidialer_campaign_pacing_up', (('campaign_id', '19'),))),
            1.0,
        )
        self.assertEqual(
            samples.get(('omnidialer_campaign_calls', (('campaign_id', '19'),))),
            1.0,
        )
        self.assertEqual(
            samples.get((
                'omnidialer_campaign_mode',
                (('campaign_id', '19'), ('mode', 'PREDICTIVE')),
            )),
            1.0,
        )
        self.assertEqual(samples.get(('omnidialer_metrics_scrape_ok', ())), 1.0)

    def test_collect_redis_error_sets_scrape_ok_zero(self):
        bad = MagicMock()
        bad.scan_iter.side_effect = RuntimeError('redis down')
        collector = metrics.DialerPacingCollector(bad)
        samples = _samples(collector)
        self.assertEqual(samples.get(('omnidialer_metrics_scrape_ok', ())), 0.0)


if __name__ == '__main__':
    unittest.main()
