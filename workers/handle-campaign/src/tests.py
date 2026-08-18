# -*- coding: utf-8 -*-

import datetime
import unittest
import uuid
from unittest.mock import MagicMock
from decimal import Decimal
import psycopg
import time
import json
import math

from datetime import timedelta
from gearman.job import GearmanJob
from gearman.worker import GearmanWorker

from handler.naive import (
    AverageWorker, SchedulerWorker, ACTIVE, PAUSED, CREATED, FINALIZED,
    STATUS_SELECTED_CALL, STATUS_CREATED, FINALIZED_NOCONTACT, STATUS_AMD_MACHINE,
    STATUS_TERMINATED, STATUS_EXIT_ABANDON, STATUS_EXIT_TIMEOUT,
    AUDIT_LOCK_KEY, AUDIT_ACTIVE_CHANNELS_JOB,
    PHASE_RINGING, PHASE_WAITING_AGENT, PHASE_ONCALL,
)
import handler.naive as naive_mod
import settings.default as dialer_settings
import os


class PredictiveConstantsTests(unittest.TestCase):
    """Defaults and helpers for predictive pacing knobs."""

    def test_settings_defaults(self):
        self.assertEqual(dialer_settings.MAX_ABANDON_RATE, 0.03)
        self.assertEqual(dialer_settings.WARM_UP_SAMPLE_SIZE, 50)
        self.assertEqual(dialer_settings.PREDICTIVE_TICK_MS, 1000)
        self.assertEqual(dialer_settings.HIT_RATE_FLOOR, 0.05)
        self.assertEqual(dialer_settings.DROP_RATE_ALPHA, 0.1)
        self.assertEqual(dialer_settings.DEFAULT_ART_SEC, 15.0)
        self.assertEqual(dialer_settings.P_LIB_REMAINING_EPS, 1.0)
        self.assertTrue(dialer_settings.PREDICTIVE_ENABLED)
        self.assertEqual(dialer_settings.GAMMA_THROTTLE_FLOOR, 0.2)
        self.assertEqual(dialer_settings.PACING_SNAPSHOT_TTL_SEC, 30)
        self.assertEqual(dialer_settings.THROTTLE_STREAK_K, 5)
        self.assertEqual(dialer_settings.THROTTLE_EXIT_RATIO, 0.8)
        self.assertEqual(naive_mod.MAX_ABANDON_RATE, dialer_settings.MAX_ABANDON_RATE)
        self.assertEqual(naive_mod.WARM_UP_SAMPLE_SIZE, dialer_settings.WARM_UP_SAMPLE_SIZE)
        self.assertEqual(naive_mod.PREDICTIVE_TICK_MS, dialer_settings.PREDICTIVE_TICK_MS)
        self.assertEqual(naive_mod.HIT_RATE_FLOOR, dialer_settings.HIT_RATE_FLOOR)
        self.assertEqual(naive_mod.DROP_RATE_ALPHA, dialer_settings.DROP_RATE_ALPHA)
        self.assertEqual(naive_mod.DEFAULT_ART_SEC, dialer_settings.DEFAULT_ART_SEC)
        self.assertEqual(naive_mod.P_LIB_REMAINING_EPS, dialer_settings.P_LIB_REMAINING_EPS)
        self.assertEqual(naive_mod.PREDICTIVE_ENABLED, dialer_settings.PREDICTIVE_ENABLED)
        self.assertEqual(naive_mod.GAMMA_THROTTLE_FLOOR, dialer_settings.GAMMA_THROTTLE_FLOOR)
        self.assertEqual(
            naive_mod.PACING_SNAPSHOT_TTL_SEC, dialer_settings.PACING_SNAPSHOT_TTL_SEC,
        )
        self.assertEqual(naive_mod.THROTTLE_STREAK_K, dialer_settings.THROTTLE_STREAK_K)
        self.assertEqual(naive_mod.THROTTLE_EXIT_RATIO, dialer_settings.THROTTLE_EXIT_RATIO)

    def test_is_predictive_warmup_uses_att_count(self):
        orig = AverageWorker.get_campaign_att_count
        try:
            AverageWorker.get_campaign_att_count = MagicMock(return_value=10)
            self.assertTrue(AverageWorker.is_predictive_warmup(4))
            AverageWorker.get_campaign_att_count = MagicMock(
                return_value=dialer_settings.WARM_UP_SAMPLE_SIZE,
            )
            self.assertFalse(AverageWorker.is_predictive_warmup(4))
        finally:
            AverageWorker.get_campaign_att_count = orig

    def test_get_campaign_drop_rate(self):
        orig = AverageWorker.get_campaign_metrics
        try:
            # EWMA presente gana al ratio acumulado
            AverageWorker.get_campaign_metrics = MagicMock(return_value={
                'HIT_COUNT': 100,
                'CONNECT_COUNT': 100,
                'ABANDON_COUNT': 3,
                'DROP_RATE': 0.03,
                'DROP_RATE_EWMA': 0.12,
                'HAS_DROP_RATE_EWMA': True,
            })
            self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(4), 0.12)
            # Sin métricas
            AverageWorker.get_campaign_metrics = MagicMock(return_value=None)
            self.assertIsNone(AverageWorker.get_campaign_drop_rate(4))
            # Sin muestra (HIT=0 y sin EWMA)
            AverageWorker.get_campaign_metrics = MagicMock(return_value={
                'HIT_COUNT': 0, 'CONNECT_COUNT': 0, 'ABANDON_COUNT': 0,
                'DROP_RATE': 0.0, 'DROP_RATE_EWMA': 0.0, 'HAS_DROP_RATE_EWMA': False,
            })
            self.assertIsNone(AverageWorker.get_campaign_drop_rate(4))
            # Fallback legacy: sin DROP_RATE_EWMA → ratio acumulado
            AverageWorker.get_campaign_metrics = MagicMock(return_value={
                'HIT_COUNT': 100,
                'ABANDON_COUNT': 3,
                'DROP_RATE': 0.03,
                'DROP_RATE_EWMA': 0.0,
                'HAS_DROP_RATE_EWMA': False,
            })
            self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(4), 0.03)
        finally:
            AverageWorker.get_campaign_metrics = orig

    def test_get_campaign_p_hit_floors_and_requires_sample(self):
        orig = AverageWorker.get_campaign_metrics
        try:
            AverageWorker.get_campaign_metrics = MagicMock(return_value=None)
            self.assertIsNone(AverageWorker.get_campaign_p_hit(4))
            AverageWorker.get_campaign_metrics = MagicMock(return_value={
                'HIT_COUNT': 0, 'FAIL_COUNT': 0, 'P_HIT': 0.0,
            })
            self.assertIsNone(AverageWorker.get_campaign_p_hit(4))
            AverageWorker.get_campaign_metrics = MagicMock(return_value={
                'HIT_COUNT': 1, 'FAIL_COUNT': 99, 'P_HIT': 0.01,
            })
            self.assertAlmostEqual(
                AverageWorker.get_campaign_p_hit(4),
                dialer_settings.HIT_RATE_FLOOR,
            )
            AverageWorker.get_campaign_metrics = MagicMock(return_value={
                'HIT_COUNT': 50, 'FAIL_COUNT': 50, 'P_HIT': 0.42,
            })
            self.assertAlmostEqual(AverageWorker.get_campaign_p_hit(4), 0.42)
        finally:
            AverageWorker.get_campaign_metrics = orig


class CampaignHitMetricsH3Tests(unittest.TestCase):
    """H3: P_hit / Drop semantics on CAMP:{id}:METRICS (Redis dialer DB3)."""

    campaign_id = 8044

    def setUp(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{self.campaign_id}:METRICS')

    def tearDown(self):
        try:
            AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{self.campaign_id}:METRICS')
            AverageWorker.REDIS_DIALER_CONNECTION.close()
        except Exception:
            pass

    def test_canonical_20_hits_78_fails_2_abandons(self):
        cid = self.campaign_id
        for _ in range(20):
            AverageWorker.update_campaign_hit(cid, hit=True)
        for _ in range(78):
            AverageWorker.update_campaign_hit(cid, hit=False, abandon=False)
        for _ in range(2):
            AverageWorker.update_campaign_hit(cid, hit=False, abandon=True)

        metrics = AverageWorker.get_campaign_metrics(cid)
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics['HIT_COUNT'], 20)
        self.assertEqual(metrics['FAIL_COUNT'], 78)
        self.assertEqual(metrics['ABANDON_COUNT'], 2)
        self.assertEqual(metrics['CONNECT_COUNT'], 20)
        self.assertAlmostEqual(metrics['P_HIT_RATIO'], 20.0 / 98.0, places=5)
        self.assertAlmostEqual(metrics['DROP_RATE'], 0.10, places=5)
        # EWMA simétrico: 2 abandones tras hits → 0.1 luego 0.19; fuente de γ
        self.assertTrue(metrics['HAS_DROP_RATE_EWMA'])
        self.assertAlmostEqual(metrics['DROP_RATE_EWMA'], 0.19, places=5)
        self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(cid), 0.19, places=5)

        raw_drop = AverageWorker.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{cid}:METRICS', 'DROP_RATE',
        )
        self.assertAlmostEqual(float(raw_drop), 0.10, places=5)
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.hget(f'CAMP:{cid}:METRICS', 'WINDOW_MODE'),
            'ewma_symmetric',
        )

    def test_abandon_is_not_fail_and_does_not_move_p_hit(self):
        cid = self.campaign_id
        AverageWorker.update_campaign_hit(cid, hit=True)
        before = AverageWorker.get_campaign_metrics(cid)
        p_before = float(before['P_HIT'])
        AverageWorker.update_campaign_hit(cid, hit=False, abandon=True)
        after = AverageWorker.get_campaign_metrics(cid)
        self.assertEqual(after['FAIL_COUNT'], 0)
        self.assertEqual(after['ABANDON_COUNT'], 1)
        self.assertEqual(after['HIT_COUNT'], 1)
        self.assertEqual(after['CONNECT_COUNT'], 1)
        self.assertAlmostEqual(float(after['P_HIT']), p_before, places=6)
        self.assertAlmostEqual(float(after['DROP_RATE']), 1.0, places=5)
        self.assertAlmostEqual(float(after['DROP_RATE_EWMA']), 0.1, places=5)
        self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(cid), 0.1, places=5)

    def test_connect_count_equals_hits_not_attempts(self):
        cid = self.campaign_id
        AverageWorker.update_campaign_hit(cid, hit=True)
        AverageWorker.update_campaign_hit(cid, hit=False, abandon=False)
        AverageWorker.update_campaign_hit(cid, hit=False, abandon=True)
        metrics = AverageWorker.get_campaign_metrics(cid)
        self.assertEqual(metrics['CONNECT_COUNT'], 1)
        self.assertEqual(metrics['HIT_COUNT'], 1)
        self.assertEqual(metrics['FAIL_COUNT'], 1)
        self.assertEqual(metrics['ABANDON_COUNT'], 1)
        self.assertAlmostEqual(metrics['DROP_RATE'], 1.0, places=5)
        self.assertAlmostEqual(metrics['P_HIT_RATIO'], 0.5, places=5)

    def test_drop_rate_ewma_symmetric_decay_and_recovery(self):
        """Racha de abandones sube el EWMA; hits lo decaen ×0.9 y cura un mal arranque."""
        cid = self.campaign_id
        alpha = dialer_settings.DROP_RATE_ALPHA
        expected = 0.0
        for _ in range(5):
            AverageWorker.update_campaign_hit(cid, hit=False, abandon=True)
            expected = expected + alpha * (1.0 - expected)
        metrics = AverageWorker.get_campaign_metrics(cid)
        self.assertAlmostEqual(metrics['DROP_RATE_EWMA'], expected, places=5)
        self.assertAlmostEqual(expected, 0.40951, places=4)
        self.assertGreaterEqual(
            AverageWorker.get_campaign_drop_rate(cid),
            dialer_settings.MAX_ABANDON_RATE,
        )

        hits_needed = 0
        while AverageWorker.get_campaign_drop_rate(cid) >= dialer_settings.MAX_ABANDON_RATE:
            AverageWorker.update_campaign_hit(cid, hit=True)
            hits_needed += 1
            self.assertLess(hits_needed, 40, 'EWMA should recover under D_max within ~33 hits')
        # Con α=0.1 y 5 abandones: ~25 hits para bajar de ~0.41 a <0.03
        self.assertGreaterEqual(hits_needed, 20)
        self.assertLessEqual(hits_needed, 30)
        # Fails no deben mover el EWMA
        before = AverageWorker.get_campaign_drop_rate(cid)
        AverageWorker.update_campaign_hit(cid, hit=False, abandon=False)
        self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(cid), before, places=6)

    def test_drop_rate_legacy_fallback_then_ewma_takes_over(self):
        """Hash sin DROP_RATE_EWMA usa ratio acumulado; tras un evento el EWMA manda."""
        cid = self.campaign_id
        key = f'CAMP:{cid}:METRICS'
        AverageWorker.REDIS_DIALER_CONNECTION.hset(
            key,
            mapping={
                'HIT_COUNT': '100',
                'CONNECT_COUNT': '100',
                'FAIL_COUNT': '0',
                'ABANDON_COUNT': '5',
                'DROP_RATE': '0.05',
                'P_HIT': '0.5',
                'P_HIT_RATIO': '1.0',
                'WINDOW_MODE': 'ewma',
            },
        )
        metrics = AverageWorker.get_campaign_metrics(cid)
        self.assertFalse(metrics['HAS_DROP_RATE_EWMA'])
        self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(cid), 0.05, places=5)

        AverageWorker.update_campaign_hit(cid, hit=True)
        after = AverageWorker.get_campaign_metrics(cid)
        self.assertTrue(after['HAS_DROP_RATE_EWMA'])
        self.assertEqual(after['WINDOW_MODE'], 'ewma_symmetric')
        # Hit sin EWMA previo → 0 + α*(0-0) = 0 (toma el mando)
        self.assertAlmostEqual(after['DROP_RATE_EWMA'], 0.0, places=5)
        self.assertAlmostEqual(AverageWorker.get_campaign_drop_rate(cid), 0.0, places=5)
        # Ratio acumulado sigue en reporting (5/101)
        self.assertAlmostEqual(after['DROP_RATE'], 5.0 / 101.0, places=5)

    def test_hit_update_does_not_overwrite_amd_time(self):
        """HIT lua no pisa METRICS.AMD_TIME (H7 lo escribe solo vía AmdLatency)."""
        cid = self.campaign_id
        key = f'CAMP:{cid}:METRICS'
        AverageWorker.REDIS_DIALER_CONNECTION.hset(key, 'AMD_TIME', '2.5')
        AverageWorker.update_campaign_hit(cid, hit=True)
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.hget(key, 'AMD_TIME'),
            '2.5',
        )


class CampaignAmdLatencyH7Tests(unittest.TestCase):
    """H7: AMD_LATENCY + t_ring + process_event AmdLatency."""

    campaign_id = 8050

    def setUp(self):
        AverageWorker.connect_redis_dialer()
        redis_d = AverageWorker.REDIS_DIALER_CONNECTION
        redis_d.delete(
            f'CAMP:{self.campaign_id}:AMD_LATENCY',
            f'CAMP:{self.campaign_id}:METRICS',
            f'CAMP:{self.campaign_id}:ART',
        )
        AverageWorker._campaign_amd_cache.clear()
        AverageWorker._amd_conf_fallback_cache = (0.0, None)
        self.worker = MagicMock()
        self._orig_insert = AverageWorker.insert_job
        self._orig_remove = AverageWorker.remove_job
        AverageWorker.insert_job = MagicMock(return_value=1)
        AverageWorker.remove_job = MagicMock()

    def tearDown(self):
        AverageWorker.insert_job = self._orig_insert
        AverageWorker.remove_job = self._orig_remove
        AverageWorker._campaign_amd_cache.clear()
        AverageWorker._amd_conf_fallback_cache = (0.0, None)
        try:
            AverageWorker.REDIS_DIALER_CONNECTION.delete(
                f'CAMP:{self.campaign_id}:AMD_LATENCY',
                f'CAMP:{self.campaign_id}:METRICS',
                f'CAMP:{self.campaign_id}:ART',
            )
            AverageWorker.REDIS_DIALER_CONNECTION.close()
        except Exception:
            pass

    def test_update_campaign_amd_latency_sets_avg_and_metrics(self):
        cid = self.campaign_id
        AverageWorker._update_campaign_amd_latency(cid, 2.0)
        AverageWorker._update_campaign_amd_latency(cid, 4.0)
        lat = f'CAMP:{cid}:AMD_LATENCY'
        self.assertAlmostEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(lat, 'AMD_SUM')), 6.0,
        )
        self.assertEqual(
            int(AverageWorker.REDIS_DIALER_CONNECTION.hget(lat, 'AMD_COUNT')), 2,
        )
        self.assertAlmostEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(lat, 'AMD')), 3.0,
        )
        self.assertAlmostEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(
                f'CAMP:{cid}:METRICS', 'AMD_TIME')),
            3.0,
        )

    def test_process_event_amd_latency_updates_without_contact_fields(self):
        cid = self.campaign_id
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = {
            'type': 'AmdLatency',
            'id_campaign': str(cid),
            'amd_duration': 2.5,
            'callid': 'amd-lat-1',
        }
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding='UTF8'),
        )
        AverageWorker.process_event(self.worker, job)
        self.assertAlmostEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(
                f'CAMP:{cid}:AMD_LATENCY', 'AMD')),
            2.5,
        )
        AverageWorker.GM_CLIENT.submit_job.assert_not_called()

    def test_t_ring_amd_off_is_art_only(self):
        orig_amd = AverageWorker.campaign_has_amd
        try:
            AverageWorker.campaign_has_amd = MagicMock(return_value=False)
            AverageWorker.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{self.campaign_id}:ART',
                mapping={'ART_SUM': '10', 'ART_COUNT': '1', 'ART': '10'},
            )
            self.assertAlmostEqual(
                AverageWorker.get_campaign_t_ring(self.campaign_id), 10.0,
            )
        finally:
            AverageWorker.campaign_has_amd = orig_amd

    def test_t_ring_amd_on_fallback_without_sample(self):
        orig_amd = AverageWorker.campaign_has_amd
        orig_fb = AverageWorker.get_amd_config_fallback_sec
        try:
            AverageWorker.campaign_has_amd = MagicMock(return_value=True)
            AverageWorker.get_amd_config_fallback_sec = MagicMock(return_value=5.0)
            AverageWorker.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{self.campaign_id}:ART',
                mapping={'ART_SUM': '10', 'ART_COUNT': '1', 'ART': '10'},
            )
            self.assertAlmostEqual(
                AverageWorker.get_campaign_t_ring(self.campaign_id), 15.0,
            )
        finally:
            AverageWorker.campaign_has_amd = orig_amd
            AverageWorker.get_amd_config_fallback_sec = orig_fb

    def test_t_ring_amd_on_uses_measured_avg(self):
        orig_amd = AverageWorker.campaign_has_amd
        try:
            AverageWorker.campaign_has_amd = MagicMock(return_value=True)
            AverageWorker.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{self.campaign_id}:ART',
                mapping={'ART_SUM': '8', 'ART_COUNT': '1', 'ART': '8'},
            )
            AverageWorker._update_campaign_amd_latency(self.campaign_id, 2.0)
            AverageWorker._update_campaign_amd_latency(self.campaign_id, 4.0)
            self.assertAlmostEqual(
                AverageWorker.get_campaign_t_ring(self.campaign_id), 11.0,
            )
        finally:
            AverageWorker.campaign_has_amd = orig_amd


class CampaignPacingSnapshotTests(unittest.TestCase):
    """P2: CAMP:{id}:PACING snapshot written with TTL."""

    campaign_id = 8045

    def setUp(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{self.campaign_id}:PACING')

    def tearDown(self):
        try:
            AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{self.campaign_id}:PACING')
            AverageWorker.REDIS_DIALER_CONNECTION.close()
        except Exception:
            pass

    def test_publish_campaign_pacing_sets_hash_and_ttl(self):
        cid = self.campaign_id
        AverageWorker._publish_campaign_pacing(
            cid,
            MODE='PREDICTIVE',
            REASON='ok',
            GAMMA=1.0,
            C_DIAL=4,
            P_HIT=0.5,
            DROP_RATE=0.01,
            A_FREE=2,
            A_EXPECTED=1.4,
            C_RINGING=3,
        )
        key = f'CAMP:{cid}:PACING'
        raw = AverageWorker.REDIS_DIALER_CONNECTION.hgetall(key)
        self.assertEqual(raw.get('MODE'), 'PREDICTIVE')
        self.assertEqual(raw.get('REASON'), 'ok')
        self.assertEqual(raw.get('GAMMA'), '1.0')
        self.assertEqual(raw.get('C_DIAL'), '4')
        self.assertEqual(raw.get('P_HIT'), '0.5')
        self.assertEqual(raw.get('DROP_RATE'), '0.01')
        self.assertEqual(raw.get('A_FREE'), '2')
        self.assertEqual(raw.get('A_EXPECTED'), '1.4')
        self.assertEqual(raw.get('C_RINGING'), '3')
        self.assertTrue(int(raw.get('TS') or 0) > 0)
        ttl = AverageWorker.REDIS_DIALER_CONNECTION.ttl(key)
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, dialer_settings.PACING_SNAPSHOT_TTL_SEC)

    def test_publish_campaign_pacing_none_fields_as_empty(self):
        cid = self.campaign_id
        AverageWorker._publish_campaign_pacing(
            cid,
            MODE='PREDICTIVE_WARMUP',
            REASON='warmup',
            GAMMA=0.0,
            C_DIAL=0,
            P_HIT=None,
            DROP_RATE=None,
            A_FREE=1,
            A_EXPECTED=0.0,
            C_RINGING=0,
        )
        raw = AverageWorker.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{cid}:PACING')
        self.assertEqual(raw.get('P_HIT'), '')
        self.assertEqual(raw.get('DROP_RATE'), '')
        self.assertEqual(raw.get('MODE'), 'PREDICTIVE_WARMUP')


class PredictiveStatsHtmxTests(unittest.TestCase):
    """Métricas predictivas expuestas en la vista HTMX admin (render_template)."""

    campaign_id = 8047

    PREDICTIVE_KEYS = [
        'CAMP:{0}:PACING', 'CAMP:{0}:METRICS', 'CAMP:{0}:ART', 'CAMP:{0}:ACW',
        'CAMP:{0}:AHT', 'CAMP:{0}:AMD_LATENCY', 'CAMP:{0}:CHANNELS',
    ]

    def setUp(self):
        AverageWorker.connect_redis_dialer()
        for key in self.PREDICTIVE_KEYS:
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key.format(self.campaign_id))

    def tearDown(self):
        try:
            for key in self.PREDICTIVE_KEYS:
                AverageWorker.REDIS_DIALER_CONNECTION.delete(key.format(self.campaign_id))
            AverageWorker.REDIS_DIALER_CONNECTION.close()
        except Exception:
            pass

    def test_collect_predictive_stats_reads_all_hashes(self):
        cid = self.campaign_id
        conn = AverageWorker.REDIS_DIALER_CONNECTION
        conn.hset(f'CAMP:{cid}:PACING', mapping={'MODE': 'PREDICTIVE', 'GAMMA': '1.0'})
        conn.hset(f'CAMP:{cid}:METRICS', mapping={'HIT_COUNT': '10', 'DROP_RATE_EWMA': '0.02'})
        conn.hset(f'CAMP:{cid}:ART', mapping={'ART': '12.5', 'ART_COUNT': '10'})
        conn.hset(f'CAMP:{cid}:ACW', mapping={'ACW': '30.0'})
        conn.hset(f'CAMP:{cid}:AHT', mapping={'AHT': '120.0'})
        conn.hset(f'CAMP:{cid}:AMD_LATENCY', mapping={'AMD': '2.5', 'AMD_COUNT': '4'})
        conn.hset(f'CAMP:{cid}:CHANNELS', mapping={'RINGING': '2', 'ONCALL': '3', 'TOTAL': '5'})

        sections = AverageWorker.collect_predictive_stats(cid)
        self.assertEqual(set(sections.keys()), {
            'pacing', 'metrics', 'art', 'acw', 'aht', 'amd_latency', 'channels',
        })
        self.assertEqual(sections['pacing'].get('MODE'), 'PREDICTIVE')
        self.assertEqual(sections['metrics'].get('HIT_COUNT'), '10')
        self.assertEqual(sections['art'].get('ART'), '12.5')
        self.assertEqual(sections['acw'].get('ACW'), '30.0')
        self.assertEqual(sections['aht'].get('AHT'), '120.0')
        self.assertEqual(sections['amd_latency'].get('AMD'), '2.5')
        self.assertEqual(sections['channels'].get('TOTAL'), '5')

    def test_collect_predictive_stats_empty_campaign_returns_empty_sections(self):
        sections = AverageWorker.collect_predictive_stats(self.campaign_id)
        self.assertEqual(set(sections.keys()), {
            'pacing', 'metrics', 'art', 'acw', 'aht', 'amd_latency', 'channels',
        })
        for section in sections.values():
            self.assertEqual(section, {})

    def test_render_pacing_html_includes_sections_and_values(self):
        from ui.rendering import AdminRender
        cid = self.campaign_id
        AverageWorker.REDIS_DIALER_CONNECTION.hset(
            f'CAMP:{cid}:PACING', mapping={'MODE': 'PREDICTIVE', 'C_DIAL': '4'})
        html = AdminRender.render_pacing(cid, AverageWorker.collect_predictive_stats(cid))
        self.assertIn('Predictive pacing', html)
        self.assertIn('PREDICTIVE', html)
        self.assertIn('C_DIAL', html)
        self.assertIn('No data available', html)  # secciones sin hash

    def test_render_stats_html_includes_pacing_poll(self):
        from ui.rendering import AdminRender
        cid = self.campaign_id
        html = AdminRender.render_stats(
            cid, {'ATTEMPTED_CALLS': '7'},
            predictive=AverageWorker.collect_predictive_stats(cid))
        self.assertIn('ATTEMPTED_CALLS', html)
        self.assertIn(f'/htmx/pacing/{cid}', html)
        self.assertIn('every 5s', html)


class CampaignThrottleStreakTests(unittest.TestCase):
    """P3: kill-switch streak + latch hysteresis on Redis."""

    campaign_id = 8046

    def setUp(self):
        AverageWorker.connect_redis_dialer()
        r = AverageWorker.REDIS_DIALER_CONNECTION
        r.delete(
            f'CAMP:{self.campaign_id}:THROTTLE_STREAK',
            f'CAMP:{self.campaign_id}:THROTTLE_LATCH',
        )

    def tearDown(self):
        try:
            r = AverageWorker.REDIS_DIALER_CONNECTION
            r.delete(
                f'CAMP:{self.campaign_id}:THROTTLE_STREAK',
                f'CAMP:{self.campaign_id}:THROTTLE_LATCH',
            )
            r.close()
        except Exception:
            pass

    def test_streak_reaches_k_engages_latch(self):
        cid = self.campaign_id
        d_max = dialer_settings.MAX_ABANDON_RATE
        k = dialer_settings.THROTTLE_STREAK_K
        event = None
        result = None
        for i in range(k):
            result = AverageWorker._update_throttle_streak(cid, 0.05, d_max)
            if i < k - 1:
                self.assertFalse(result['force_throttle'])
                self.assertFalse(result['latched'])
                self.assertEqual(result['streak'], i + 1)
                self.assertIsNone(result['event'])
            else:
                event = result['event']
        self.assertTrue(result['force_throttle'])
        self.assertTrue(result['latched'])
        self.assertEqual(result['streak'], k)
        self.assertEqual(event, 'THROTTLE_ENGAGED')
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.get(
                f'CAMP:{cid}:THROTTLE_LATCH',
            ),
            '1',
        )

    def test_reset_streak_when_drop_below_dmax_without_latch(self):
        cid = self.campaign_id
        d_max = dialer_settings.MAX_ABANDON_RATE
        AverageWorker._update_throttle_streak(cid, 0.05, d_max)
        AverageWorker._update_throttle_streak(cid, 0.05, d_max)
        result = AverageWorker._update_throttle_streak(cid, 0.01, d_max)
        self.assertEqual(result['streak'], 0)
        self.assertFalse(result['latched'])
        self.assertFalse(result['force_throttle'])
        self.assertIsNone(result['event'])

    def test_hysteresis_keeps_latch_until_exit_ratio(self):
        cid = self.campaign_id
        d_max = dialer_settings.MAX_ABANDON_RATE
        exit_thr = dialer_settings.THROTTLE_EXIT_RATIO * d_max
        # Engage latch
        for _ in range(dialer_settings.THROTTLE_STREAK_K):
            AverageWorker._update_throttle_streak(cid, 0.05, d_max)
        # Between exit_thr and D_max → stay latched
        mid = (exit_thr + d_max) / 2.0
        held = AverageWorker._update_throttle_streak(cid, mid, d_max)
        self.assertTrue(held['latched'])
        self.assertTrue(held['force_throttle'])
        self.assertIsNone(held['event'])
        # Below exit → clear
        cleared = AverageWorker._update_throttle_streak(cid, exit_thr * 0.5, d_max)
        self.assertFalse(cleared['latched'])
        self.assertFalse(cleared['force_throttle'])
        self.assertEqual(cleared['event'], 'THROTTLE_CLEARED')
        self.assertEqual(cleared['streak'], 0)


class DialModePacingTests(unittest.TestCase):
    """Unit tests for power / progressive / predictive dial-mode dispatch."""

    campaign_id = 99

    def setUp(self):
        self._redis = {}
        self._orig_redis = AverageWorker.REDIS_DIALER_CONNECTION
        mock_redis = MagicMock()

        def redis_get(key):
            return self._redis.get(key)

        def redis_set(key, value):
            self._redis[key] = value
            return True

        mock_redis.get.side_effect = redis_get
        mock_redis.set.side_effect = redis_set
        mock_redis.hgetall.return_value = {}
        AverageWorker.REDIS_DIALER_CONNECTION = mock_redis

        self._orig_get_predictive = AverageWorker.get_predictive_model
        self._orig_get_boost = AverageWorker.get_boost_factor
        self._orig_get_agents = AverageWorker.get_number_available_agents
        self._orig_get_snapshot = AverageWorker.get_campaign_agent_snapshot
        self._orig_get_active = AverageWorker.get_active_channels
        self._orig_get_max = AverageWorker.get_campaign_max_available_channels
        self._orig_get_phases = AverageWorker.get_campaign_channel_phases
        self._orig_get_att_count = AverageWorker.get_campaign_att_count
        self._orig_get_drop = AverageWorker.get_campaign_drop_rate
        self._orig_get_p_hit = AverageWorker.get_campaign_p_hit
        self._orig_get_metrics = AverageWorker.get_campaign_metrics
        self._orig_get_a_expected = AverageWorker.get_campaign_a_expected
        self._orig_publish_pacing = AverageWorker._publish_campaign_pacing
        AverageWorker._publish_campaign_pacing = MagicMock()
        self._orig_update_throttle = AverageWorker._update_throttle_streak
        AverageWorker._update_throttle_streak = MagicMock(return_value={
            'streak': 0,
            'latched': False,
            'force_throttle': False,
            'event': None,
        })
        try:
            AverageWorker.get_predictive_model.cache_clear()
            AverageWorker.get_boost_factor.cache_clear()
        except AttributeError:
            pass

    def tearDown(self):
        AverageWorker.REDIS_DIALER_CONNECTION = self._orig_redis
        AverageWorker.get_predictive_model = self._orig_get_predictive
        AverageWorker.get_boost_factor = self._orig_get_boost
        AverageWorker.get_number_available_agents = self._orig_get_agents
        AverageWorker.get_campaign_agent_snapshot = self._orig_get_snapshot
        AverageWorker.get_active_channels = self._orig_get_active
        AverageWorker.get_campaign_max_available_channels = self._orig_get_max
        AverageWorker.get_campaign_channel_phases = self._orig_get_phases
        AverageWorker.get_campaign_att_count = self._orig_get_att_count
        AverageWorker.get_campaign_drop_rate = self._orig_get_drop
        AverageWorker.get_campaign_p_hit = self._orig_get_p_hit
        AverageWorker.get_campaign_metrics = self._orig_get_metrics
        AverageWorker.get_campaign_a_expected = self._orig_get_a_expected
        AverageWorker._publish_campaign_pacing = self._orig_publish_pacing
        AverageWorker._update_throttle_streak = self._orig_update_throttle
        try:
            AverageWorker.get_predictive_model.cache_clear()
            AverageWorker.get_boost_factor.cache_clear()
        except AttributeError:
            pass

    def _set_power_keys(self, customdialerdst='0', voicebot='False'):
        cid = self.campaign_id
        self._redis[f'CAMP:{cid}:CUSTOMDIALERDST'] = customdialerdst
        self._redis[f'CAMP:{cid}:VOICEBOT'] = voicebot

    def test_resolve_dial_mode_power_by_customdialerdst(self):
        self._set_power_keys(customdialerdst='SIP/trunk')
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        mode, reason = AverageWorker.resolve_dial_mode(self.campaign_id)
        self.assertEqual(mode, AverageWorker.DIAL_MODE_POWER)
        self.assertIn('CUSTOMDIALERDST', reason)

    def test_resolve_dial_mode_power_by_voicebot_overrides_predictive(self):
        self._set_power_keys(customdialerdst='0', voicebot='True')
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        mode, reason = AverageWorker.resolve_dial_mode(self.campaign_id)
        self.assertEqual(mode, AverageWorker.DIAL_MODE_POWER)
        self.assertEqual(reason, 'VOICEBOT=True')

    def test_resolve_dial_mode_predictive(self):
        self._set_power_keys()
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        mode, reason = AverageWorker.resolve_dial_mode(self.campaign_id)
        self.assertEqual(mode, AverageWorker.DIAL_MODE_PREDICTIVE)
        self.assertEqual(reason, 'initial_predictive_model=True')

    def test_resolve_dial_mode_progressive(self):
        self._set_power_keys()
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        mode, reason = AverageWorker.resolve_dial_mode(self.campaign_id)
        self.assertEqual(mode, AverageWorker.DIAL_MODE_PROGRESSIVE)
        self.assertEqual(reason, 'initial_predictive_model=False')

    def _mock_pacing_inputs(
            self, agents_score=2.0, active=2, max_channels=10,
            ringing=None, waiting=0, oncall=0):
        AverageWorker.get_number_available_agents = MagicMock(
            return_value=(agents_score, int(agents_score)))
        AverageWorker.get_active_channels = MagicMock(return_value=active)
        AverageWorker.get_campaign_max_available_channels = MagicMock(
            return_value=max_channels)
        if ringing is None:
            # Default: treat "active" as unassigned so legacy progressive
            # expectations stay stable when callers do not pass phases.
            ringing = active
        AverageWorker.get_campaign_channel_phases = MagicMock(return_value={
            PHASE_RINGING: ringing,
            PHASE_WAITING_AGENT: waiting,
            PHASE_ONCALL: oncall,
            'TOTAL': ringing + waiting + oncall,
        })

    def test_allowed_parallel_power_returns_channel_headroom(self):
        self._set_power_keys(customdialerdst='exten')
        self._mock_pacing_inputs(agents_score=0, active=3, max_channels=10)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=2.0)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 7)

    def test_allowed_parallel_progressive_uses_boost_factor(self):
        self._set_power_keys()
        self._mock_pacing_inputs(agents_score=2.0, active=2, max_channels=10)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=2.0)
        # target = ceil(2*2)=4, unassigned=2 → calls_to_dial=2
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 2)

    def test_allowed_parallel_progressive_one_agent_dials_one(self):
        """Progressive R=1: 1 READY and 0 in-flight → exactly 1 originate."""
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=0, max_channels=4, ringing=0, oncall=0)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 1)

    def test_allowed_parallel_progressive_at_target_returns_zero(self):
        """No keep-alive: 1 READY and 1 RINGING → 0 (do not race to max_channels)."""
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=1, max_channels=4, ringing=1, oncall=0)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 0)

    def test_allowed_parallel_progressive_over_target_returns_zero(self):
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=3, max_channels=4, ringing=3, oncall=0)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 0)

    def test_allowed_parallel_progressive_ready_ignores_oncall(self):
        """1 READY + 1 ONCALL (other agent busy) → dial 1 for the free agent."""
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=1, max_channels=5, ringing=0, oncall=1)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 1)

    def test_allowed_parallel_progressive_ready_waiting_counts(self):
        """WAITING_AGENT also consumes READY quota (like RINGING)."""
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=1, max_channels=5, ringing=0, waiting=1,
            oncall=0)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 0)

    def test_allowed_parallel_predictive_warmup_at_target_returns_zero(self):
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=2.0, active=2, max_channels=10, ringing=2, oncall=0)
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=2.0)
        snapshot_mock = MagicMock(return_value={
            'a_free': 2,
            'total_ready': 2,
            'a_oncall': 0.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 0.0,
            'total_oncall': 0,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [],
        })
        AverageWorker.get_campaign_agent_snapshot = snapshot_mock
        AverageWorker.get_campaign_att_count = MagicMock(return_value=0)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=None)
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=None)
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 0.0, 't_ring': 15.0, 'aht': 0.0, 'att': 0.0, 'acw': 0.0,
            'details': [],
        })
        # warm-up → progressive R=1 → target=2, unassigned=2 → 0
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 0)
        AverageWorker.get_boost_factor.assert_called()
        snapshot_mock.assert_called_once_with(self.campaign_id)
        AverageWorker._publish_campaign_pacing.assert_called_once()
        pub_kwargs = AverageWorker._publish_campaign_pacing.call_args.kwargs
        self.assertEqual(pub_kwargs['MODE'], 'PREDICTIVE_WARMUP')
        self.assertEqual(pub_kwargs['C_DIAL'], 0)
        self.assertEqual(pub_kwargs['REASON'], 'warmup')
        self.assertEqual(pub_kwargs['GAMMA'], 0.0)

    def test_allowed_parallel_predictive_warmup_ready_ignores_oncall(self):
        """Warm-up R=1: READY must dial even if another agent is ONCALL."""
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=1, max_channels=5, ringing=0, oncall=1)
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.5)
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 1,
            'total_ready': 1,
            'a_oncall': 1.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 1.0,
            'total_oncall': 1,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [{'agent_id': 1}],
        })
        AverageWorker.get_campaign_att_count = MagicMock(return_value=3)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=0.0)
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=0.5)
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 0.9, 't_ring': 6.0, 'aht': 50.0, 'att': 30.0, 'acw': 20.0,
            'details': [],
        })
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 1)
        pub_kwargs = AverageWorker._publish_campaign_pacing.call_args.kwargs
        self.assertEqual(pub_kwargs['MODE'], 'PREDICTIVE_WARMUP')
        self.assertEqual(pub_kwargs['C_DIAL'], 1)

    def test_resolve_dial_mode_predictive_disabled_by_ff(self):
        self._set_power_keys()
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        orig = naive_mod.PREDICTIVE_ENABLED
        try:
            naive_mod.PREDICTIVE_ENABLED = False
            mode, reason = AverageWorker.resolve_dial_mode(self.campaign_id)
            self.assertEqual(mode, AverageWorker.DIAL_MODE_PROGRESSIVE)
            self.assertIn('DIALER_PREDICTIVE_ENABLED=false', reason)
        finally:
            naive_mod.PREDICTIVE_ENABLED = orig

    def test_allowed_parallel_predictive_cdial_when_warmed_up(self):
        """Post warm-up: C_dial from formula, capped by channel headroom."""
        self._set_power_keys()
        # active=2, max=10 → headroom=8
        self._mock_pacing_inputs(agents_score=2.0, active=2, max_channels=10)
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 2,
            'total_ready': 2,
            'a_oncall': 0.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 0.0,
            'total_oncall': 0,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [],
        })
        AverageWorker.get_campaign_channel_phases = MagicMock(return_value={
            PHASE_RINGING: 0,
            PHASE_WAITING_AGENT: 0,
            PHASE_ONCALL: 0,
            'TOTAL': 2,
        })
        AverageWorker.get_campaign_att_count = MagicMock(return_value=100)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=0.01)
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=0.5)
        AverageWorker.get_campaign_metrics = MagicMock(return_value={
            'P_HIT_RATIO': 0.5, 'HIT_COUNT': 20, 'FAIL_COUNT': 20, 'ABANDON_COUNT': 0,
        })
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 0.0, 't_ring': 15.0, 'aht': 60.0, 'att': 50.0, 'acw': 10.0,
            'details': [],
        })
        # C_dial = floor(((2+0-0)/0.5)*1) = 4; headroom=8 → 4
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 4)
        AverageWorker._publish_campaign_pacing.assert_called_once()
        pub_kwargs = AverageWorker._publish_campaign_pacing.call_args.kwargs
        self.assertEqual(pub_kwargs['MODE'], 'PREDICTIVE')
        self.assertEqual(pub_kwargs['C_DIAL'], 4)
        self.assertEqual(pub_kwargs['P_HIT'], 0.5)
        self.assertEqual(pub_kwargs['DROP_RATE'], 0.01)
        self.assertEqual(pub_kwargs['A_FREE'], 2)
        self.assertEqual(pub_kwargs['REASON'], 'ok')

    def test_allowed_parallel_predictive_throttled_when_drop_ge_dmax(self):
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=2.0, active=2, max_channels=10, ringing=2, oncall=0)
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=2.0)
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 2,
            'total_ready': 2,
            'a_oncall': 0.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 0.0,
            'total_oncall': 0,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [],
        })
        AverageWorker.get_campaign_att_count = MagicMock(return_value=100)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=0.05)  # > 0.03
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=0.5)
        AverageWorker.get_campaign_metrics = MagicMock(return_value={})
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 10.0, 't_ring': 15.0, 'aht': 60.0, 'att': 50.0, 'acw': 10.0,
            'details': [],
        })
        AverageWorker._update_throttle_streak = MagicMock(return_value={
            'streak': dialer_settings.THROTTLE_STREAK_K,
            'latched': True,
            'force_throttle': True,
            'event': 'THROTTLE_ENGAGED',
        })
        # latched throttle → progressive R=1 → target=2, unassigned=2 → 0
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 0)
        AverageWorker._publish_campaign_pacing.assert_called_once()
        pub_kwargs = AverageWorker._publish_campaign_pacing.call_args.kwargs
        self.assertEqual(pub_kwargs['MODE'], 'PREDICTIVE_THROTTLED')
        self.assertEqual(pub_kwargs['C_DIAL'], 0)
        self.assertEqual(pub_kwargs['GAMMA'], 0.0)
        self.assertEqual(pub_kwargs['REASON'], 'throttle_streak')
        self.assertEqual(pub_kwargs['DROP_RATE'], 0.05)
        self.assertEqual(pub_kwargs['EVENT'], 'THROTTLE_ENGAGED')
        self.assertEqual(pub_kwargs['THROTTLE_LATCHED'], 1)

    def test_allowed_parallel_predictive_throttled_ready_ignores_oncall(self):
        """Throttled R=1 still dials for READY when only other agent is ONCALL."""
        self._set_power_keys()
        self._mock_pacing_inputs(
            agents_score=1.0, active=1, max_channels=5, ringing=0, oncall=1)
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.5)
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 1,
            'total_ready': 1,
            'a_oncall': 1.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 1.0,
            'total_oncall': 1,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [{'agent_id': 1}],
        })
        AverageWorker.get_campaign_att_count = MagicMock(return_value=100)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=0.17)
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=0.9)
        AverageWorker.get_campaign_metrics = MagicMock(return_value={})
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 0.98, 't_ring': 6.0, 'aht': 50.0, 'att': 30.0, 'acw': 20.0,
            'details': [],
        })
        AverageWorker._update_throttle_streak = MagicMock(return_value={
            'streak': dialer_settings.THROTTLE_STREAK_K,
            'latched': True,
            'force_throttle': True,
            'event': None,
        })
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 1)
        pub_kwargs = AverageWorker._publish_campaign_pacing.call_args.kwargs
        self.assertEqual(pub_kwargs['MODE'], 'PREDICTIVE_THROTTLED')
        self.assertEqual(pub_kwargs['C_DIAL'], 1)

    def test_allowed_parallel_predictive_soft_hold_when_drop_ge_dmax_no_latch(self):
        """D≥D_max sin latch: soft floor (γ=floor), still predictive C_dial>0."""
        self._set_power_keys()
        self._mock_pacing_inputs(agents_score=2.0, active=2, max_channels=10)
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 2,
            'total_ready': 2,
            'a_oncall': 0.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 0.0,
            'total_oncall': 0,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [],
        })
        AverageWorker.get_campaign_channel_phases = MagicMock(return_value={
            PHASE_RINGING: 0,
            PHASE_WAITING_AGENT: 0,
            PHASE_ONCALL: 0,
            'TOTAL': 2,
        })
        AverageWorker.get_campaign_att_count = MagicMock(return_value=100)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=0.05)
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=0.5)
        AverageWorker.get_campaign_metrics = MagicMock(return_value={})
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 0.0, 't_ring': 15.0, 'aht': 60.0, 'att': 50.0, 'acw': 10.0,
            'details': [],
        })
        AverageWorker._update_throttle_streak = MagicMock(return_value={
            'streak': 2,
            'latched': False,
            'force_throttle': False,
            'event': None,
        })
        # soft: γ=0.2 → C_dial = floor(((2+0)/0.5)*0.2)=floor(0.8)=0
        # Need higher free to get dial>0: with a_free=2, floor((4)*0.2)=0
        # a_free=5 → floor((10)*0.2)=2
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 5,
            'total_ready': 5,
            'a_oncall': 0.0,
            'a_postcall': 0.0,
            'a_pause_acw': 0.0,
            'a_busy': 0.0,
            'total_oncall': 0,
            'total_postcall': 0,
            'total_pause_acw': 0,
            'busy_agents': [],
        })
        self._mock_pacing_inputs(
            agents_score=5.0, active=2, max_channels=20, ringing=0, oncall=2)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 2)
        pub_kwargs = AverageWorker._publish_campaign_pacing.call_args.kwargs
        self.assertEqual(pub_kwargs['MODE'], 'PREDICTIVE')
        self.assertEqual(pub_kwargs['REASON'], 'drop_over_dmax_soft')
        self.assertAlmostEqual(float(pub_kwargs['GAMMA']), 0.2)
        self.assertEqual(pub_kwargs['THROTTLE_STREAK'], 2)
        self.assertEqual(pub_kwargs['THROTTLE_LATCHED'], 0)

    def test_allowed_parallel_no_agents_returns_zero(self):
        self._set_power_keys()
        self._mock_pacing_inputs(agents_score=0, active=0, max_channels=10)
        AverageWorker.get_predictive_model = MagicMock(return_value=False)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.5)
        allowed = AverageWorker.allowed_parallel_contact_attempts(self.campaign_id)
        self.assertEqual(allowed, 0)


class PredictivePacerUnitTests(unittest.TestCase):
    """Pure H5 formula tests (no Redis)."""

    def test_compute_c_dial_table(self):
        from handler.predictive_pacer import compute_c_dial
        # a_free=2, a_exp=0, ringing=0, p=0.5, g=1 → 4
        self.assertEqual(compute_c_dial(2, 0, 0, 0.5, 1.0), 4)
        # a_free=0, a_exp=4.5, ringing=0, p=0.5 → 9
        self.assertEqual(compute_c_dial(0, 4.5, 0, 0.5, 1.0), 9)
        # ringing cancels free capacity: (2+0-4*0.5)/0.5 = 0
        self.assertEqual(compute_c_dial(2, 0, 4, 0.5, 1.0), 0)
        # gamma 0 → 0
        self.assertEqual(compute_c_dial(2, 0, 0, 0.5, 0.0), 0)
        # negative raw clamped
        self.assertEqual(compute_c_dial(0, 0, 10, 0.5, 1.0), 0)

    def test_compute_gamma_zones(self):
        from handler.predictive_pacer import compute_gamma
        d_max = 0.03
        self.assertEqual(compute_gamma(None, d_max, aggressiveness=1.0), 1.0)
        self.assertEqual(compute_gamma(0.01, d_max, aggressiveness=1.0), 1.0)
        self.assertEqual(compute_gamma(0.05, d_max, aggressiveness=1.0), 0.0)
        # mid zone: d=0.0225 is midway between 0.015 and 0.03 → midway 1.0→0.2 = 0.6
        mid = compute_gamma(0.0225, d_max, aggressiveness=1.0, gamma_floor=0.2)
        self.assertAlmostEqual(mid, 0.6, places=5)
        # aggressiveness as high-end
        self.assertEqual(compute_gamma(0.0, d_max, aggressiveness=2.0), 2.0)

    def test_decide_predictive_pace_warmup_and_throttle(self):
        from handler.predictive_pacer import decide_predictive_pace
        warm = decide_predictive_pace(
            a_free=2, a_expected=5, c_ringing=0, p_hit=0.5, drop_rate=0.01,
            d_max=0.03, aggressiveness=1.0, warmup=True,
        )
        self.assertTrue(warm['use_progressive_r1'])
        self.assertEqual(warm['mode'], 'warmup')

        # D≥D_max without force_throttle → soft predictive (γ=floor)
        soft = decide_predictive_pace(
            a_free=2, a_expected=5, c_ringing=0, p_hit=0.5, drop_rate=0.05,
            d_max=0.03, aggressiveness=1.0, warmup=False, gamma_floor=0.2,
        )
        self.assertFalse(soft['use_progressive_r1'])
        self.assertEqual(soft['mode'], 'predictive')
        self.assertEqual(soft['reason'], 'drop_over_dmax_soft')
        self.assertAlmostEqual(soft['gamma'], 0.2)

        # force_throttle → hard kill-switch
        throttled = decide_predictive_pace(
            a_free=2, a_expected=5, c_ringing=0, p_hit=0.5, drop_rate=0.05,
            d_max=0.03, aggressiveness=1.0, warmup=False, force_throttle=True,
        )
        self.assertTrue(throttled['use_progressive_r1'])
        self.assertEqual(throttled['mode'], 'throttled')
        self.assertEqual(throttled['reason'], 'throttle_streak')

        ok = decide_predictive_pace(
            a_free=2, a_expected=0, c_ringing=0, p_hit=0.5, drop_rate=0.01,
            d_max=0.03, aggressiveness=1.0, warmup=False,
        )
        self.assertFalse(ok['use_progressive_r1'])
        self.assertEqual(ok['mode'], 'predictive')
        self.assertEqual(ok['c_dial'], 4)

    def test_apply_channel_caps(self):
        from handler.predictive_pacer import apply_channel_caps
        self.assertEqual(apply_channel_caps(10, 3), 3)
        self.assertEqual(apply_channel_caps(2, 8), 2)
        self.assertEqual(apply_channel_caps(5, 0), 0)


class CampaignAgentSnapshotTests(unittest.TestCase):
    """Unit tests for READY + busy agent snapshot used by predictive pacing."""

    campaign_id = 77

    def setUp(self):
        self._agent_hashes = {}
        self._orig_oml_redis = AverageWorker.REDIS_OML_CONNECTION
        self._orig_connect = AverageWorker.connect_redis_oml
        self._orig_get_ids = AverageWorker.get_agent_ids_campaign
        self._orig_dialer_redis = AverageWorker.REDIS_DIALER_CONNECTION
        self._orig_get_predictive = AverageWorker.get_predictive_model
        self._orig_get_agents = AverageWorker.get_number_available_agents
        self._orig_get_snapshot = AverageWorker.get_campaign_agent_snapshot
        self._orig_get_active = AverageWorker.get_active_channels
        self._orig_get_max = AverageWorker.get_campaign_max_available_channels
        self._orig_get_phases = AverageWorker.get_campaign_channel_phases
        self._orig_get_att_count = AverageWorker.get_campaign_att_count
        self._orig_get_drop = AverageWorker.get_campaign_drop_rate
        self._orig_get_a_expected = AverageWorker.get_campaign_a_expected
        self._orig_get_boost = AverageWorker.get_boost_factor
        self._orig_get_p_hit = AverageWorker.get_campaign_p_hit
        self._orig_get_metrics = AverageWorker.get_campaign_metrics

        mock_redis = MagicMock()

        def hmget(key, *fields):
            data = self._agent_hashes.get(key, {})
            return [data.get(f) for f in fields]

        mock_redis.hmget.side_effect = hmget
        AverageWorker.REDIS_OML_CONNECTION = mock_redis
        AverageWorker.connect_redis_oml = MagicMock()

    def tearDown(self):
        AverageWorker.REDIS_OML_CONNECTION = self._orig_oml_redis
        AverageWorker.connect_redis_oml = self._orig_connect
        AverageWorker.get_agent_ids_campaign = self._orig_get_ids
        AverageWorker.REDIS_DIALER_CONNECTION = self._orig_dialer_redis
        AverageWorker.get_predictive_model = self._orig_get_predictive
        AverageWorker.get_number_available_agents = self._orig_get_agents
        AverageWorker.get_campaign_agent_snapshot = self._orig_get_snapshot
        AverageWorker.get_active_channels = self._orig_get_active
        AverageWorker.get_campaign_max_available_channels = self._orig_get_max
        AverageWorker.get_campaign_channel_phases = self._orig_get_phases
        AverageWorker.get_campaign_att_count = self._orig_get_att_count
        AverageWorker.get_campaign_drop_rate = self._orig_get_drop
        AverageWorker.get_campaign_a_expected = self._orig_get_a_expected
        AverageWorker.get_boost_factor = self._orig_get_boost
        AverageWorker.get_campaign_p_hit = self._orig_get_p_hit
        AverageWorker.get_campaign_metrics = self._orig_get_metrics

    def _set_agent(self, agent_id, status, timestamp=None):
        payload = {'STATUS': status}
        if timestamp is not None:
            payload['TIMESTAMP'] = str(timestamp)
        self._agent_hashes[f'OML:AGENT:{agent_id}'] = payload

    def test_snapshot_ready_agent(self):
        AverageWorker.get_agent_ids_campaign = MagicMock(return_value={1: 1})
        self._set_agent(1, 'READY', timestamp=100)
        snap = AverageWorker.get_campaign_agent_snapshot(self.campaign_id)
        self.assertEqual(snap['a_free'], 1)
        self.assertEqual(snap['total_ready'], 1)
        self.assertEqual(snap['a_busy'], 0.0)
        self.assertEqual(snap['busy_agents'], [])

    def test_snapshot_busy_statuses_with_elapsed(self):
        AverageWorker.get_agent_ids_campaign = MagicMock(
            return_value={1: 1, 2: 1, 3: 1})
        now = int(time.time())
        self._set_agent(1, 'ONCALL', timestamp=now - 40)
        self._set_agent(2, 'POSTCALL', timestamp=now - 15)
        self._set_agent(3, 'PAUSE-ACW', timestamp=now - 8)
        snap = AverageWorker.get_campaign_agent_snapshot(self.campaign_id)
        self.assertEqual(snap['total_oncall'], 1)
        self.assertEqual(snap['total_postcall'], 1)
        self.assertEqual(snap['total_pause_acw'], 1)
        self.assertEqual(snap['a_oncall'], 1.0)
        self.assertEqual(snap['a_postcall'], 1.0)
        self.assertEqual(snap['a_pause_acw'], 1.0)
        self.assertEqual(snap['a_busy'], 3.0)
        self.assertEqual(len(snap['busy_agents']), 3)
        by_status = {item['status']: item for item in snap['busy_agents']}
        self.assertAlmostEqual(by_status['ONCALL']['elapsed_sec'], 40, delta=2)
        self.assertAlmostEqual(by_status['POSTCALL']['elapsed_sec'], 15, delta=2)
        self.assertAlmostEqual(by_status['PAUSE-ACW']['elapsed_sec'], 8, delta=2)

    def test_snapshot_multiqueue_weight(self):
        AverageWorker.get_agent_ids_campaign = MagicMock(return_value={9: 2})
        self._set_agent(9, 'ONCALL', timestamp=int(time.time()) - 5)
        snap = AverageWorker.get_campaign_agent_snapshot(self.campaign_id)
        self.assertEqual(snap['total_oncall'], 1)
        self.assertEqual(snap['a_oncall'], 0.5)
        self.assertEqual(snap['a_busy'], 0.5)
        self.assertEqual(snap['busy_agents'][0]['weight'], 0.5)

    def test_snapshot_ignores_unrelated_pause(self):
        AverageWorker.get_agent_ids_campaign = MagicMock(return_value={4: 1})
        self._set_agent(4, 'PAUSE-xyz', timestamp=int(time.time()) - 3)
        snap = AverageWorker.get_campaign_agent_snapshot(self.campaign_id)
        self.assertEqual(snap['a_busy'], 0.0)
        self.assertEqual(snap['busy_agents'], [])
        self.assertEqual(snap['a_free'], 0)

    def test_get_number_available_agents_uses_snapshot(self):
        AverageWorker.get_agent_ids_campaign = MagicMock(return_value={5: 1})
        self._set_agent(5, 'READY')
        a_free, total = AverageWorker.get_number_available_agents(self.campaign_id)
        self.assertEqual(a_free, 1)
        self.assertEqual(total, 1)

    def test_predictive_uses_a_expected_in_cdial(self):
        """Post warm-up: busy snapshot + A_expected feeds C_dial (not progressive R=1)."""
        cid = 88
        dialer_keys = {
            f'CAMP:{cid}:CUSTOMDIALERDST': '0',
            f'CAMP:{cid}:VOICEBOT': 'False',
        }
        mock_dialer = MagicMock()
        mock_dialer.get.side_effect = lambda key: dialer_keys.get(key)
        mock_dialer.hgetall.return_value = {}
        mock_dialer.hget.return_value = None
        AverageWorker.REDIS_DIALER_CONNECTION = mock_dialer
        AverageWorker.get_predictive_model = MagicMock(return_value=True)
        AverageWorker.get_boost_factor = MagicMock(return_value=1.0)
        AverageWorker.get_active_channels = MagicMock(return_value=2)
        AverageWorker.get_campaign_max_available_channels = MagicMock(
            return_value=10)
        AverageWorker.get_number_available_agents = MagicMock(
            return_value=(2, 2))
        AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
            'a_free': 2,
            'total_ready': 2,
            'a_oncall': 1.0,
            'a_postcall': 0.5,
            'a_pause_acw': 0.5,
            'a_busy': 2.0,
            'total_oncall': 1,
            'total_postcall': 1,
            'total_pause_acw': 1,
            'busy_agents': [
                {'agent_id': 1, 'status': 'ONCALL', 'elapsed_sec': 30,
                 'weight': 1.0},
            ],
        })
        AverageWorker.get_campaign_channel_phases = MagicMock(return_value={
            PHASE_RINGING: 1,
            PHASE_WAITING_AGENT: 0,
            PHASE_ONCALL: 1,
            'TOTAL': 2,
        })
        AverageWorker.get_campaign_att_count = MagicMock(return_value=100)
        AverageWorker.get_campaign_drop_rate = MagicMock(return_value=0.01)
        AverageWorker.get_campaign_p_hit = MagicMock(return_value=0.5)
        AverageWorker.get_campaign_metrics = MagicMock(return_value={
            'P_HIT_RATIO': 0.5, 'HIT_COUNT': 10, 'FAIL_COUNT': 10,
        })
        AverageWorker.get_campaign_a_expected = MagicMock(return_value={
            'a_expected': 0.42,
            't_ring': 15.0,
            'aht': 60.0,
            'att': 50.0,
            'acw': 10.0,
            'details': [
                {'agent_id': 1, 'status': 'ONCALL', 'elapsed_sec': 30,
                 'weight': 1.0, 'p_lib': 0.42, 'remaining_sec': 30.0,
                 'contribution': 0.42},
            ],
        })
        # floor(((2+0.42-1*0.5)/0.5)*1)=floor(3.84)=3; headroom=8 → 3
        allowed = AverageWorker.allowed_parallel_contact_attempts(cid)
        self.assertEqual(allowed, 3)
        AverageWorker.get_campaign_agent_snapshot.assert_called_once_with(cid)
        AverageWorker.get_campaign_a_expected.assert_called_once()


class AExpectedPLibTests(unittest.TestCase):
    """H4: P_lib exponencial y A_expected ponderado."""

    def test_p_lib_oncall_near_end_of_aht_is_high(self):
        # remaining = max(1, 60-55) = 5; horizon=15 → 1-exp(-3) ≈ 0.9502
        p_lib = AverageWorker.compute_agent_p_lib(
            'ONCALL', elapsed_sec=55, horizon_sec=15,
            aht=60.0, att=50.0, acw=10.0,
        )
        self.assertGreater(p_lib, 0.9)
        self.assertAlmostEqual(p_lib, 1.0 - math.exp(-15.0 / 5.0), places=5)

    def test_p_lib_oncall_just_started_is_low(self):
        p_lib = AverageWorker.compute_agent_p_lib(
            'ONCALL', elapsed_sec=2, horizon_sec=15,
            aht=60.0, att=50.0, acw=10.0,
        )
        # remaining=58 → 1-exp(-15/58) ≈ 0.228
        self.assertLess(p_lib, 0.3)
        self.assertGreater(p_lib, 0.1)

    def test_p_lib_zero_without_timing_stats(self):
        self.assertEqual(
            AverageWorker.compute_agent_p_lib(
                'ONCALL', elapsed_sec=10, horizon_sec=15,
                aht=0.0, att=0.0, acw=0.0,
            ),
            0.0,
        )

    def test_p_lib_pause_unrelated_is_zero(self):
        self.assertEqual(
            AverageWorker.compute_agent_p_lib(
                'PAUSE-break', elapsed_sec=100, horizon_sec=15,
                aht=60.0, att=50.0, acw=10.0,
            ),
            0.0,
        )

    def test_p_lib_postcall_uses_acw(self):
        # ACW=10, elapsed=9 → remaining=1; horizon=15 → ~1.0
        p_lib = AverageWorker.compute_agent_p_lib(
            'POSTCALL', elapsed_sec=9, horizon_sec=15,
            aht=60.0, att=50.0, acw=10.0,
        )
        self.assertGreater(p_lib, 0.99)

    def test_a_expected_five_oncall_near_end(self):
        busy = [
            {'agent_id': i, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 1.0}
            for i in range(1, 6)
        ]
        a_exp, details = AverageWorker.compute_a_expected(
            busy, horizon_sec=15, aht=60.0, att=50.0, acw=10.0,
        )
        self.assertEqual(len(details), 5)
        self.assertGreater(a_exp, 4.0)
        self.assertAlmostEqual(a_exp, 5 * details[0]['p_lib'], places=5)

    def test_a_expected_multiqueue_weight(self):
        busy = [
            {'agent_id': 1, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 0.5},
        ]
        a_exp, details = AverageWorker.compute_a_expected(
            busy, horizon_sec=15, aht=60.0, att=50.0, acw=10.0,
        )
        self.assertAlmostEqual(a_exp, 0.5 * details[0]['p_lib'], places=5)

    def test_a_expected_zero_when_only_unrelated_pause(self):
        # Snapshot already filters PAUSE-*; empty busy → 0
        a_exp, details = AverageWorker.compute_a_expected(
            [], horizon_sec=15, aht=60.0, att=50.0, acw=10.0,
        )
        self.assertEqual(a_exp, 0.0)
        self.assertEqual(details, [])

    def test_get_campaign_a_expected_uses_snapshot_and_timing(self):
        orig_snap = AverageWorker.get_campaign_agent_snapshot
        orig_att = AverageWorker.get_campaign_att
        orig_acw = AverageWorker.get_campaign_acw
        orig_aht = AverageWorker.get_campaign_aht
        orig_tring = AverageWorker.get_campaign_t_ring
        try:
            AverageWorker.get_campaign_agent_snapshot = MagicMock(return_value={
                'busy_agents': [
                    {'agent_id': 1, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 1.0},
                    {'agent_id': 2, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 1.0},
                    {'agent_id': 3, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 1.0},
                    {'agent_id': 4, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 1.0},
                    {'agent_id': 5, 'status': 'ONCALL', 'elapsed_sec': 55, 'weight': 1.0},
                ],
                'a_free': 0,
            })
            AverageWorker.get_campaign_att = MagicMock(return_value=50.0)
            AverageWorker.get_campaign_acw = MagicMock(return_value=10.0)
            AverageWorker.get_campaign_aht = MagicMock(return_value=60.0)
            AverageWorker.get_campaign_t_ring = MagicMock(return_value=15.0)
            result = AverageWorker.get_campaign_a_expected(77)
            self.assertGreater(result['a_expected'], 4.0)
            self.assertEqual(result['t_ring'], 15.0)
            self.assertEqual(result['aht'], 60.0)
            self.assertEqual(len(result['details']), 5)
            AverageWorker.get_campaign_agent_snapshot.assert_called_once_with(77)
        finally:
            AverageWorker.get_campaign_agent_snapshot = orig_snap
            AverageWorker.get_campaign_att = orig_att
            AverageWorker.get_campaign_acw = orig_acw
            AverageWorker.get_campaign_aht = orig_aht
            AverageWorker.get_campaign_t_ring = orig_tring

    def test_get_campaign_t_ring_falls_back_to_default_art(self):
        orig_art = AverageWorker.get_campaign_art
        orig_amd = AverageWorker.get_campaign_amd_time
        try:
            AverageWorker.get_campaign_art = MagicMock(return_value=None)
            AverageWorker.get_campaign_amd_time = MagicMock(return_value=0.0)
            self.assertAlmostEqual(
                AverageWorker.get_campaign_t_ring(9),
                dialer_settings.DEFAULT_ART_SEC,
            )
            AverageWorker.get_campaign_art = MagicMock(return_value=12.0)
            AverageWorker.get_campaign_amd_time = MagicMock(return_value=3.0)
            self.assertAlmostEqual(AverageWorker.get_campaign_t_ring(9), 15.0)
        finally:
            AverageWorker.get_campaign_art = orig_art
            AverageWorker.get_campaign_amd_time = orig_amd


class CampaignAhtUnitTests(unittest.TestCase):
    """Unit tests for derived AHT = ATT + ACW without full process-event stack."""

    def setUp(self):
        self._store = {}
        self._orig_redis = AverageWorker.REDIS_DIALER_CONNECTION
        self._orig_connect = AverageWorker.connect_redis_dialer
        mock_redis = MagicMock()

        def hget(key, field):
            return self._store.get((key, field))

        def hset(key, field, value):
            self._store[(key, field)] = value
            return 1

        mock_redis.hget.side_effect = hget
        mock_redis.hset.side_effect = hset
        AverageWorker.REDIS_DIALER_CONNECTION = mock_redis
        AverageWorker.connect_redis_dialer = MagicMock()

    def tearDown(self):
        AverageWorker.REDIS_DIALER_CONNECTION = self._orig_redis
        AverageWorker.connect_redis_dialer = self._orig_connect

    def test_refresh_campaign_aht_sums_att_and_acw(self):
        self._store[('CAMP:7:ATT', 'ATT')] = '40'
        self._store[('CAMP:7:ACW', 'ACW')] = '10'
        AverageWorker._refresh_campaign_aht(7)
        self.assertEqual(float(self._store[('CAMP:7:AHT', 'AHT')]), 50.0)

    def test_refresh_campaign_aht_missing_side_is_zero(self):
        self._store[('CAMP:7:ATT', 'ATT')] = '40'
        AverageWorker._refresh_campaign_aht(7)
        self.assertEqual(float(self._store[('CAMP:7:AHT', 'AHT')]), 40.0)

    def test_update_att_refreshes_aht(self):
        orig_refresh = AverageWorker._refresh_campaign_aht
        AverageWorker._refresh_campaign_aht = MagicMock()
        AverageWorker.REDIS_DIALER_CONNECTION.eval = MagicMock()
        try:
            AverageWorker._update_campaign_att(7, 12.0)
            AverageWorker.REDIS_DIALER_CONNECTION.eval.assert_called_once()
            AverageWorker._refresh_campaign_aht.assert_called_once_with(7)
        finally:
            AverageWorker._refresh_campaign_aht = orig_refresh

    def test_update_acw_refreshes_aht(self):
        orig_refresh = AverageWorker._refresh_campaign_aht
        AverageWorker._refresh_campaign_aht = MagicMock()
        AverageWorker.REDIS_DIALER_CONNECTION.eval = MagicMock()
        try:
            AverageWorker._update_campaign_acw(7, 8.0)
            AverageWorker.REDIS_DIALER_CONNECTION.eval.assert_called_once()
            AverageWorker._refresh_campaign_aht.assert_called_once_with(7)
        finally:
            AverageWorker._refresh_campaign_aht = orig_refresh


class MyTestSuite(unittest.TestCase):

    def setUp(self):
        self._wait_pg()
        self.fetchmany_counter = 0
        self._create_campaign()

    def tearDown(self):
        self.clean_databases()

    @classmethod
    def encode_payload(cls, data):
        return bytes(json.dumps(data), encoding="UTF8")

    def clean_databases(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.flushdb()
        self._wait_pg()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('DELETE FROM campaign;')
            cursor_dialer.execute('DELETE FROM contact;')
            cursor_dialer.execute('DELETE FROM incidence_rules;')
            cursor_dialer.execute('DELETE FROM incidence_rules_disposition;')
            cursor_dialer.execute('DELETE FROM contact_in_campaign;')
            cursor_dialer.execute('UPDATE system_control SET is_active = true;')
            cursor_dialer.execute('DELETE FROM jobs;')
        AverageWorker.REDIS_DIALER_CONNECTION.close()

    def mocked_get_contacts_campaign(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            return [(1, '6093017590',
                     '["Amanda Jenkins", "Gregory Henson", "7147034", "4067530816", "5273724517"]',
                     True),
                    (2, '5143016455',
                     '["Ashley Barrett", "Edward Townsend", "8718745", "5618936401", "1075763364"]',
                     True)]
        return []

    def mocked_get_contacts_campaign_no_phone(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            return [(1, '',
                     '["Amanda Jenkins", "Gregory Henson", "7147034", "4067530816", "5273724517"]',
                     True),
                    (2, '5143016455',
                     '["Ashley Barrett", "Edward Townsend", "8718745", "5618936401", "1075763364"]',
                     True)]
        return []

    def _wait_pg(self, attempts: int = 40, delay: float = 0.25):
        """
        Espera a que Postgres de tests acepte conexiones para evitar:
        'connection refused ... port 5434'
        """
        for _ in range(attempts):
            try:
                with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR):
                    return
            except Exception:
                time.sleep(delay)
        raise RuntimeError("Postgres for tests not ready")

    def _create_campaign(self):
        # mocking Postgres connection to OML
        AverageWorker.get_oml_connection = MagicMock()
        # mocking get_campaign_data
        self.campaign_id_data = (
            4, 2, 'test_dialer_01', datetime.date(2024, 8, 21),
            datetime.datetime.now().date(), 2, 10, 'rrmemory', 10, False, Decimal('1.0'), 1, False,
            True, False, False, False, False, False, datetime.time(15, 51), datetime.time(15, 51),
            [1, 3, 4], 1,
            '"{\\"prim_fila_enc\\": false, \\"cant_col\\": 6, \\"nombres_de_columnas\\": '
            '[\\"telefono\\", \\"nombre\\", \\"apellido\\", \\"dni\\", \\"telefono2\\", '
            '\\"telefono3\\"], \\"cols_telefono\\": [0, 4, 5]}"', '0')
        self.incidence_rules_data = [(1, 1, 'busy', 4, 20, 1, 4), (2, 4, 'congestion', 3, 40, 1, 4)]
        self.incidence_rules_disposition_data = [(1, 7, 3, 17, 1, 4), (2, 8, 5, 7, 2, 4)]
        campaign_mocked_data = (self.campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(
            return_value=campaign_mocked_data)
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign)
        self.worker = GearmanWorker()
        job = GearmanJob(None, None, b'create-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "contact_strategy": [1, 3, 4], "prefix": ""}')
        AverageWorker.create_campaign(self.worker, job)

    def test_clean_broken_selected_contacts_starting_campaing(self):
        # mark one contact to SELECT_CALL status
        # run start_campaign
        # ensure the contact has now CREATED status
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s'
                                  ' AND id_campaign = %s;',
                                  (STATUS_SELECTED_CALL, 1, 4))
        job = GearmanJob(None, None, b'start-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "sync_omnileads": "false"}')
        AverageWorker.start_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM contact_in_campaign WHERE id_campaign = 4'
                                  ' AND status = %s;', (STATUS_CREATED,))
        self.assertEqual(cursor_dialer.fetchone()[0], 2)

    def test_clean_broken_selected_contacts_resuming_campaing(self):
        # mark one contact to SELECT_CALL status
        # run resume_campaign
        # ensure the contact has now CREATED status
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s'
                                  ' AND id_campaign = %s;',
                                  (STATUS_SELECTED_CALL, 1, 4))
        job = GearmanJob(None, None, b'resume-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "sync_omnileads": "false"}')
        AverageWorker.resume_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM contact_in_campaign WHERE id_campaign = 4'
                                  ' AND status = %s;', (STATUS_CREATED,))
        self.assertEqual(cursor_dialer.fetchone()[0], 2)

    def gen_fail_event(self, event, call_type='to_pstn'):
        return {'type': 'Dial',
                'timestamp': '2025-04-15T11:21:29.168-0300',
                'id_campaign': '4',
                'contact_id': '1',
                'phone_number': '6093017590',
                'call_type': call_type,
                'dialstatus': event,
                'forward': '',
                'dialstring': '123456720@pstn_gateway',
                'peer': {'id': '1744726885.6',
                         'name': 'PJSIP/pstn_gateway-00000006',
                         'state': 'Down',
                         'protocol_id': '108c4adb-09f6-4271-94bc-4d2a9cf468b4',
                         'caller': {'name': '4_1_6093017590', 'number': ''},
                         'connected': {'name': '1_1_6093017590', 'number': ''},
                         'accountcode': '',
                         'dialplan': {'context': 'from-omlacd',
                                      'exten': 's',
                                      'priority': 1,
                                      'app_name': 'AppDial2',
                                      'app_data': '(Outgoing Line)'},
                         'creationtime': '2025-04-15T11:21:25.125-0300',
                         'language': 'en'},
                'asterisk_id': '26:ce:a5:36:bc:0a', 'application': 'call_manager_dialer'}

    def test_get_contact_data_reads_explicit_fields(self):
        event = {
            "id_campaign": "4",
            "contact_id": "1",
            "phone_number": "6093017590",
        }
        self.assertEqual(
            AverageWorker.get_contact_data(event),
            ("4", "1", "6093017590"),
        )

    def test_get_contact_data_missing_fields_raises(self):
        event = {
            "peer": {"caller": {"name": "4_1_6093017590"}}
        }
        with self.assertRaises(KeyError):
            AverageWorker.get_contact_data(event)

    def test_chanunavailable_events(self):
        # make sure if an chanunavailable event came to process event a call won't be scheduled
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        chanunavail_event = self.gen_fail_event('CHANUNAVAIL')
        job = GearmanJob(None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps(chanunavail_event), encoding="UTF8"))
        AverageWorker.process_event(self.worker, job)
        self.assertNotEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                            'schedule-agenda')

    def test_incidence_rules(self):
        # make sure if an event came to process event and there is an incidence rule attached to it
        # it will schedule a call if the contact has still a valid number of attempts
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        busy_event = self.gen_fail_event('BUSY')
        job = GearmanJob(None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps(busy_event), encoding="UTF8"))
        AverageWorker.process_event(self.worker, job)
        # check that a job was submitted to 'schedule-contact'
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                         'schedule-agenda')

    def test_process_event_missing_explicit_fields_fails_fast(self):
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        busy_event = self.gen_fail_event('BUSY')
        busy_event.pop('id_campaign')
        job = GearmanJob(None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps(busy_event), encoding="UTF8"))
        with self.assertRaises(KeyError):
            AverageWorker.process_event(self.worker, job)

    def test_decode_fail_event_noanswer_without_dialstring(self):
        self.assertEqual(
            AverageWorker.decode_fail_event({'dialstatus': 'NOANSWER'}),
            'NOANSWER',
        )

    def test_decode_fail_event_noanswer_dialstring_none(self):
        self.assertEqual(
            AverageWorker.decode_fail_event({'dialstatus': 'NOANSWER', 'dialstring': None}),
            'NOANSWER',
        )

    def test_decode_fail_event_noanswer_agent_timeout(self):
        self.assertEqual(
            AverageWorker.decode_fail_event({
                'dialstatus': 'NOANSWER',
                'dialstring': 'camp_1@omlacd',
            }),
            'TIMEOUT',
        )

    def test_decode_fail_event_non_noanswer_passthrough(self):
        self.assertEqual(
            AverageWorker.decode_fail_event({'dialstatus': 'BUSY', 'dialstring': None}),
            'BUSY',
        )

    def test_incidence_rules_disposition(self):
        # make sure if a disposition came to the disposition endpoint and there is an incidence rule
        # disposition attached to it  will schedule a call if the contact has still a valid number
        # of attempts
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        job = GearmanJob(None, None, b'add-incidence-rule-disposition',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({"id_campaign": "4", "disposition_option": 7,
                                           "id_contact": 1}), encoding="UTF8"))
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        # check that a job was submitted to 'schedule-contact'
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args_list[0][0][0],
                         'schedule-agenda')

    def test_call_is_tagged_as_aborted_if_campaign_not_active(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;",
                                  (PAUSED,))
        job = GearmanJob(None, None, b'process-contact', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({"contact": [1, 4, 6093017590], "id_campaign": 4}),
                               encoding="utf8"))
        AverageWorker.process_contact(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("SELECT schedule_aborted FROM contact_in_campaign"
                                  " WHERE id_contact = 1;")
            self.assertEqual(cursor_dialer.fetchone()[0], True)

    def test_campaign_is_paused_if_expired(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("UPDATE campaign SET"
                                  " start_date = CURRENT_DATE - INTERVAL '2 day',"
                                  "end_date = CURRENT_DATE - INTERVAL '1 day'"
                                  " WHERE id = 4;")
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_campaign_is_forbidden_to_start_if_dialer_stopped(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE system_control SET is_active = false;")

        job = GearmanJob(None, None, b'start-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.start_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], CREATED)

    def test_campaign_is_forbidden_to_resume_if_dialer_stopped(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE system_control SET is_active = false;")
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;", (PAUSED,))

        job = GearmanJob(None, None, b'resume-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.resume_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_campaign_is_paused_if_active_after_dialer_stop(self):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("UPDATE campaign SET dialer_status = %s WHERE id = 4;", (ACTIVE,))

        job = GearmanJob(None, None, b'manage-dialer', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"action": "stop"}')
        AverageWorker.manage_dialer(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            # make sure the campaign is expired
            # and marked as PAUSED after started
            cursor_dialer.execute("SELECT dialer_status from campaign WHERE id = 4;")
            self.assertEqual(cursor_dialer.fetchone()[0], PAUSED)

    def test_campaign_is_deleted_correctly(self):
        job = GearmanJob(None, None, b'delete-campaign',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.delete_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT * from campaign;')
            self.assertEqual(cursor_dialer.fetchall(), [])

    def test_job_entry_is_removed_if_ok(self):
        job = GearmanJob(None, None, b'add-incidence-rule-disposition',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({"id_campaign": "4", "disposition_option": 7,
                                           "id_contact": 1}), encoding="UTF8"))
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT * from jobs;')
            self.assertEqual(cursor_dialer.fetchall(), [])

    def test_job_entry_is_saved_if_error(self):
        AverageWorker.clean_selected_contacts = MagicMock(side_effect=ValueError)
        payload = {
            'id_campaign': 4,
            'sync_omnileads': False
        }
        payload_bytes = self.encode_payload(payload)
        job = GearmanJob(None, None, b'start-campaign',
                         bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
        with self.assertRaises(ValueError):
            AverageWorker.start_campaign(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT * from jobs;')
            self.assertEqual(len(cursor_dialer.fetchall()), 1)

    def test_contacts_without_phone_not_imported(self):
        self.fetchmany_counter = 0
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign_no_phone)
        job = GearmanJob(None, None, b'change-database',
                         bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.change_database(self.worker, job)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) from contact_in_campaign WHERE id_campaign = 4;')
            self.assertEqual(cursor_dialer.fetchone()[0], 1)

    def test_campaign_max_available_channels_cache_invalidation(self):
        camp_id = 4
        self.assertEqual(AverageWorker.get_campaign_max_available_channels(camp_id), 1)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('UPDATE campaign SET max_channels = 3 WHERE id = %s;', (camp_id,))
        self.assertEqual(AverageWorker.get_campaign_max_available_channels(camp_id), 1)
        new_campaign_id_data = self.campaign_id_data[:11] + (3,) + self.campaign_id_data[12:]
        campaign_mocked_data = (new_campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(return_value=campaign_mocked_data)
        job = GearmanJob(None, None, b'edit-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         bytes(json.dumps({'id_campaign': '4', 'contact_strategy': [1, 3, 4]}),
                               encoding='UTF8'))
        AverageWorker.edit_campaign(self.worker, job)
        self.assertEqual(AverageWorker.get_campaign_max_available_channels(camp_id), 3)

    def test_amd_event_apply_incidence_rules(self):
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """INSERT INTO incidence_rules (id, status, status_custom, max_attempt,
                retry_later, in_mode, campaign_id) VALUES
                (%s, %s, %s, %s, %s, %s, %s);""",
                (3, STATUS_AMD_MACHINE, 'amd', 1, 7, 1, 4))
        # testing endpoint add disposition for incidence rule
        job = GearmanJob(
            None, None, b'add-incidence-rule-disposition',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{"id_campaign": "4", "disposition_option": -2, "id_contact": 1, '
            b'"phone_number": "12343556"}')

        # a call is scheduled for the first time
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
        AverageWorker.GM_CLIENT.submit_job.reset_mock()

        # a call is not scheduled for the second time because the incidence rule counter was
        # consumed
        AverageWorker.add_incidence_rule_disposition(self.worker, job)
        self.assertFalse(AverageWorker.GM_CLIENT.submit_job.called)
        AverageWorker.GM_CLIENT.submit_job.reset_mock()

    def test_suspend_campaign_schedules_call_next_day(self):
        current_date = datetime.datetime.now().date()
        extra_info = (False,                            # failed day of week match
                      0,                                # Sunday
                      False,                            # hour match
                      current_date,                     # current_date,
                      17,                               # hour,
                      9,                                # minute,
                      # campaign_info
                      (datetime.time(17, 10), datetime.time(17, 17), True, True, True, True, True,
                       True, True),
                      )
        AverageWorker.opening_hours_match = MagicMock(return_value=(False, extra_info))
        AverageWorker.set_campaign_status(4, ACTIVE)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        expected_date = current_date + timedelta(days=1)
        expected_datetime = datetime.datetime.combine(expected_date, datetime.time(17, 10))
        self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args[0][0], 'schedule-agenda')
        params_scheduler = json.loads(AverageWorker.GM_CLIENT.submit_job.call_args[0][1])
        self.assertEqual(params_scheduler['type'], 'process-campaign')
        self.assertEqual(params_scheduler['datetime_start'],
                         expected_datetime.strftime('%d/%m/%y %H:%M:%S'))

    def test_suspend_campaign_schedules_same_day(self):
        current_date = datetime.datetime.now().date()
        extra_info = (True,                             # day matches
                      0,                                # Sunday
                      False,                            # hour not matches
                      current_date,                     # current_date,
                      17,                               # hour,
                      9,                                # minute,
                      # campaign_info
                      (datetime.time(17, 10), datetime.time(17, 17), True, True, True, True, True,
                       True, True),
                      )
        AverageWorker.opening_hours_match = MagicMock(return_value=(False, extra_info))
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        AverageWorker.set_campaign_status(4, ACTIVE)
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
        self.assertEqual(AverageWorker.GM_CLIENT.submit_job.call_args[0][0], 'schedule-agenda')
        params_scheduler = json.loads(AverageWorker.GM_CLIENT.submit_job.call_args[0][1])
        self.assertEqual(params_scheduler['type'], 'process-campaign')
        expected_datetime = datetime.datetime.combine(current_date, datetime.time(17, 10))
        self.assertEqual(params_scheduler['datetime_start'],
                         expected_datetime.strftime('%d/%m/%y %H:%M:%S'))

    def test_create_campaign_sets_priority(self):
        priority = int(AverageWorker.REDIS_DIALER_CONNECTION.hget(
            'CAMP:4:DISTRIBUTION', 'PRIORITY'))
        self.assertEqual(priority, 10)  # from the initial campaign

    def test_pause_campaign_inactive_campaign_redis(self):
        AverageWorker.set_campaign_status(4, PAUSED)
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        status = int(AverageWorker.REDIS_DIALER_CONNECTION.hget(
            'CAMP:4:DISTRIBUTION', 'STATUS'))
        self.assertEqual(status, 0)

    def test_finalize_campaign_inactive_campaign_redis(self):
        AverageWorker.set_campaign_status(4, FINALIZED)
        job = GearmanJob(None, None, b'process-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4"}')
        AverageWorker.process_campaign(self.worker, job)
        status = int(AverageWorker.REDIS_DIALER_CONNECTION.hget(
            'CAMP:4:DISTRIBUTION', 'STATUS'))
        self.assertEqual(status, 0)

    def test_calculation_percentage_priority_active_ones(self):
        r = AverageWorker.REDIS_DIALER_CONNECTION
        r.hset('CAMP:1:DISTRIBUTION', 'STATUS', 0)
        r.hset('CAMP:1:DISTRIBUTION', 'PRIORITY', 3)
        r.hset('CAMP:2:DISTRIBUTION', 'STATUS', 1)
        r.hset('CAMP:2:DISTRIBUTION', 'PRIORITY', 10)
        r.hset('CAMP:3:DISTRIBUTION', 'STATUS', 1)
        r.hset('CAMP:3:DISTRIBUTION', 'PRIORITY', 5)
        r.hset('CAMP:4:DISTRIBUTION', 'STATUS', 1)
        r.hset('CAMP:4:DISTRIBUTION', 'PRIORITY', 1)

        # Sembrar campañas activas para el cálculo de porcentajes
        r.delete(AverageWorker.ACTIVE_CAMPAIGNS_SET)
        r.sadd(AverageWorker.ACTIVE_CAMPAIGNS_SET, 2, 3, 4)

        # Nueva firma: sin argumentos
        AverageWorker.update_percentages_priority_campaigns()

        percentage = float(r.hget('CAMP:4:DISTRIBUTION', 'PERCENTAGE'))
        self.assertEqual(percentage, 0.0625)

    def test_distribution_according_priority(self):
        r = AverageWorker.REDIS_DIALER_CONNECTION
        r.hset('CAMP:1:DISTRIBUTION', 'STATUS', 0)
        r.hset('CAMP:1:DISTRIBUTION', 'PRIORITY', 3)

        r.hset('CAMP:2:DISTRIBUTION', 'STATUS', 1)
        r.hset('CAMP:2:DISTRIBUTION', 'PRIORITY', 10)
        r.hset('CAMP:2:DISTRIBUTION', 'CALLS', 20)

        r.hset('CAMP:3:DISTRIBUTION', 'STATUS', 1)
        r.hset('CAMP:3:DISTRIBUTION', 'PRIORITY', 5)
        r.hset('CAMP:3:DISTRIBUTION', 'CALLS', 30)

        r.hset('CAMP:4:DISTRIBUTION', 'STATUS', 1)
        r.hset('CAMP:4:DISTRIBUTION', 'PRIORITY', 1)
        r.hset('CAMP:4:DISTRIBUTION', 'PERCENTAGE', 0.0625)

        # Sembrar campañas activas
        r.delete(AverageWorker.ACTIVE_CAMPAIGNS_SET)
        r.sadd(AverageWorker.ACTIVE_CAMPAIGNS_SET, 2, 3, 4)

        allowed_calls = AverageWorker.allowed_calls_prority_percentage(4, 50)
        self.assertEqual(allowed_calls, 6)

    def test_is_blacklisted_true(self):
        """Verifica que is_blacklisted devuelva True si Redis dice que sí"""
        AverageWorker.connect_redis_oml = MagicMock()
        # Simulamos que redis.sismember devuelve 1 (True)
        AverageWorker.REDIS_OML_CONNECTION.sismember = MagicMock(return_value=1)

        self.assertTrue(AverageWorker.is_blacklisted("11223344"))
        AverageWorker.REDIS_OML_CONNECTION.sismember.assert_called_with('OML:BLACKLIST', "11223344")

    def test_is_blacklisted_false(self):
        """Verifica que is_blacklisted devuelva False si Redis dice que no"""
        AverageWorker.connect_redis_oml = MagicMock()
        # Simulamos que redis.sismember devuelve 0 (False)
        AverageWorker.REDIS_OML_CONNECTION.sismember = MagicMock(return_value=0)

        self.assertFalse(AverageWorker.is_blacklisted("99999999"))

    def test_process_contact_skips_blacklisted(self):
        """
        Verifica que process_contact NO llame a Asterisk y marque el contacto
        como finalizado si el número está en blacklist.
        """
        # 1. Configurar Mock de Blacklist para que diga que SÍ está bloqueado
        AverageWorker.is_blacklisted = MagicMock(return_value=True)

        # 2. Mockear intento de llamada (no debería llamarse)
        AverageWorker.attempt_contact_asterisk = MagicMock()

        # 3. Crear el Job
        contact_id = 1
        phone_number = "6093017590"
        job_data = {
            "contact": [contact_id, 4, phone_number],
            "id_campaign": 4
        }
        job = GearmanJob(
            None, None, b'process-contact',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            self.encode_payload(job_data)
        )

        # 4. Ejecutar el worker
        result = AverageWorker.process_contact(self.worker, job)

        # 5. Aserciones (Verificaciones)

        # a) El worker debe devolver el mensaje de skip
        self.assertEqual(result, b'Contact skipped: Blacklisted')

        # b) NO debe haber intentado llamar a Asterisk
        AverageWorker.attempt_contact_asterisk.assert_not_called()

        # c) Debe haber actualizado la DB a FINALIZED_NOCONTACT (2)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                "SELECT final_status, schedule_aborted FROM contact_in_campaign "
                "WHERE id_contact = %s AND id_campaign = %s;",
                (contact_id, 4)
            )
            row = cursor_dialer.fetchone()
            self.assertIsNotNone(row)
            # final_status
            self.assertEqual(row[0], FINALIZED_NOCONTACT)
            # schedule_aborted false (ya se procesó)
            self.assertEqual(row[1], False)

        # Restaurar el mock original para no afectar otros tests si fuera necesario
        # (aunque en setUp se recrea gran parte, es buena práctica si es método de clase)
        del AverageWorker.is_blacklisted

    def test_handle_campaign_general(self):
        process_campaign_cm = AverageWorker.process_campaign
        AverageWorker.process_campaign = MagicMock()
        # check campaign entry creation and related tables too
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY campaign;')
            self.assertEqual(cursor_dialer.fetchone()[0], 1)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules_disposition;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY contact_in_campaign;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY contact;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)

        # let's edit the campaign now
        job = GearmanJob(None, None, b'edit-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
                         b'{"id_campaign": "4", "contact_strategy": [1, 4]}')
        campaign_id_data = self.campaign_id_data[:-4] + ([1, 4],) + self.campaign_id_data[-3:]
        campaign_mocked_data = (campaign_id_data, self.incidence_rules_data,
                                self.incidence_rules_disposition_data)
        AverageWorker.get_campaign_data = MagicMock(
            return_value=campaign_mocked_data)
        AverageWorker.edit_campaign(self.worker, job)
        # let's add an incidence rule
        job = GearmanJob(
            None, None, b'create-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{"id_campaign": 4, "id_rule": 3, "status": 3, "status_custom":"no answer", '
            b'"max_attempt": 5, "retry_later": 5, "mode": 1, "type_rule": 1}')
        AverageWorker.create_incidence_rule(self.worker, job)

        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            AverageWorker.GM_CLIENT.submit_job = MagicMock()
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules;')
            self.assertEqual(cursor_dialer.fetchone()[0], 3)
            cursor_dialer.execute('SELECT contact_strategy FROM ONLY campaign;')
            self.assertEqual(cursor_dialer.fetchone()[0], [1, 4])
            # now just edit the incidence rule
            job = GearmanJob(
                None, None, b'update-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
                b'{"id_campaign": 4, "id_rule": 3, "status": 3, "status_custom":"no answer", '
                b'"max_attempt": 7, "retry_later": 5, "mode": 1, "type_rule": 1}')
            AverageWorker.update_incidence_rule(self.worker, job)
            id_rule = 3
            cursor_dialer.execute(
                'SELECT max_attempt FROM ONLY incidence_rules WHERE id = %s;',
                (id_rule,))
            max_attempt_value = cursor_dialer.fetchone()[0]
            self.assertEqual(max_attempt_value, 7)
            # let's remove an incidence rule
            job = GearmanJob(
                None, None, b'delete-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
                b'{"id_campaign": 4, "id_rule": 3, "type_rule": 1}')
            AverageWorker.delete_incidence_rule(self.worker, job)
            cursor_dialer.execute('SELECT COUNT(*) FROM ONLY incidence_rules;')
            self.assertEqual(cursor_dialer.fetchone()[0], 2)
            id_campaign = campaign_id_data[0]
            # testing start-campaign
            AverageWorker.process_campaign = MagicMock()
            payload = {
                'id_campaign': 4,
                'sync_omnileads': False
            }
            payload_bytes = self.encode_payload(payload)
            job = GearmanJob(None, None, b'start-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.start_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, ACTIVE)

            # testing pause-campaign
            job = GearmanJob(None, None, b'pause-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.pause_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, PAUSED)

            # testing resume-campaign
            job = GearmanJob(None, None, b'resume-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.resume_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, ACTIVE)

            # testing endpoint add disposition for incidence rule
            job = GearmanJob(
                None, None, b'add-incidence-rule', bytes(str(uuid.uuid4()), encoding='utf8'),
                b'{"id_campaign": "4", "disposition_option": 8, "id_contact": 1}')
            for i in range(5):
                AverageWorker.add_incidence_rule_disposition(self.worker, job)
                self.assertTrue(AverageWorker.GM_CLIENT.submit_job.called)
                AverageWorker.GM_CLIENT.submit_job.reset_mock()
            AverageWorker.add_incidence_rule_disposition(self.worker, job)
            self.assertFalse(AverageWorker.GM_CLIENT.submit_job.called)
            AverageWorker.GM_CLIENT.submit_job.reset_mock()

            # testing stop-campaign
            job = GearmanJob(None, None, b'stop-campaign',
                             bytes(str(uuid.uuid4()), encoding='utf8'), payload_bytes)
            AverageWorker.stop_campaign(self.worker, job)
            status_campaign = AverageWorker.get_campaign_status(id_campaign, cursor_dialer)
            self.assertEqual(status_campaign, FINALIZED)

            AverageWorker.process_campaign = process_campaign_cm

    def test_originate_failed_decrements_calls(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = self.gen_fail_event('ORIGINATE_FAILED')
        event['callid'] = 'test-originate-fail-1'
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 2)

    def test_decrement_calls_once_idempotent(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 2)
        callid = 'dup-test-callid'
        AverageWorker._decrement_calls_once(4, 1, callid, context='test')
        AverageWorker._decrement_calls_once(4, 1, callid, context='test-dup')
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 1)

    def test_reset_dialer_calls_counter(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        AverageWorker.reset_dialer_calls_counter(4, reason='test')
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 0)

    def test_stop_campaign_resets_calls_counter(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        job = GearmanJob(
            None, None, b'stop-campaign', bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{"id_campaign": "4", "sync_omnileads": "false"}',
        )
        AverageWorker.stop_campaign(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 0)

    def test_audit_active_channels_corrects_ghost(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
            conn.cursor().execute(
                'UPDATE campaign SET dialer_status = %s WHERE id = 4', (FINALIZED,)
            )
        original_fetch = AverageWorker._fetch_asterisk_dialer_channel_counts
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(
            return_value=(True, {4: 0})
        )
        try:
            AverageWorker.audit_active_channels()
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 0)
        finally:
            AverageWorker._fetch_asterisk_dialer_channel_counts = original_fetch

    def test_audit_skips_when_ok_false(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        original_fetch = AverageWorker._fetch_asterisk_dialer_channel_counts
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(
            return_value=(False, {})
        )
        try:
            AverageWorker.audit_active_channels()
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 5)
        finally:
            AverageWorker._fetch_asterisk_dialer_channel_counts = original_fetch

    def test_fetch_asterisk_counts_reads_gearman_result(self):
        completed = MagicMock()
        completed.result = b'{"ok": true, "counts": {"4": 3}}'
        completed.data = None
        client = MagicMock()
        client.submit_job.return_value = completed
        original_get_client = AverageWorker._get_gearman_client
        AverageWorker._get_gearman_client = MagicMock(return_value=client)
        try:
            ok, counts, ringing = AverageWorker._fetch_asterisk_dialer_channel_counts()
            self.assertTrue(ok)
            self.assertEqual(counts, {4: 3})
            self.assertIsNone(ringing)
        finally:
            AverageWorker._get_gearman_client = original_get_client

    def test_fetch_asterisk_counts_falls_back_to_gearman_data(self):
        completed = MagicMock()
        completed.result = None
        completed.data = b'{"ok": true, "counts": {"7": 2}}'
        client = MagicMock()
        client.submit_job.return_value = completed
        original_get_client = AverageWorker._get_gearman_client
        AverageWorker._get_gearman_client = MagicMock(return_value=client)
        try:
            ok, counts, ringing = AverageWorker._fetch_asterisk_dialer_channel_counts()
            self.assertTrue(ok)
            self.assertEqual(counts, {7: 2})
            self.assertIsNone(ringing)
        finally:
            AverageWorker._get_gearman_client = original_get_client

    def test_fetch_asterisk_counts_parses_ringing_field(self):
        completed = MagicMock()
        completed.result = b'{"ok": true, "counts": {"4": 3}, "ringing": {"4": 1}}'
        completed.data = None
        client = MagicMock()
        client.submit_job.return_value = completed
        original_get_client = AverageWorker._get_gearman_client
        AverageWorker._get_gearman_client = MagicMock(return_value=client)
        try:
            ok, counts, ringing = AverageWorker._fetch_asterisk_dialer_channel_counts()
            self.assertTrue(ok)
            self.assertEqual(counts, {4: 3})
            self.assertEqual(ringing, {4: 1})
        finally:
            AverageWorker._get_gearman_client = original_get_client

    def test_audit_corrects_undercount(self):
        self._set_campaign_active(4)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 1)
        original_fetch = AverageWorker._fetch_asterisk_dialer_channel_counts
        original_reserve = AverageWorker._campaign_has_recent_reserve
        AverageWorker._campaign_has_recent_reserve = MagicMock(return_value=False)
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(
            return_value=(True, {4: 3})
        )
        try:
            AverageWorker.audit_active_channels()
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 3)
        finally:
            AverageWorker._fetch_asterisk_dialer_channel_counts = original_fetch
            AverageWorker._campaign_has_recent_reserve = original_reserve

    def test_audit_skips_overcount_with_recent_reserve(self):
        self._set_campaign_active(4)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        original_fetch = AverageWorker._fetch_asterisk_dialer_channel_counts
        original_reserve = AverageWorker._campaign_has_recent_reserve
        AverageWorker._campaign_has_recent_reserve = MagicMock(return_value=True)
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(
            return_value=(True, {4: 1})
        )
        try:
            AverageWorker.audit_active_channels()
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 5)
        finally:
            AverageWorker._fetch_asterisk_dialer_channel_counts = original_fetch
            AverageWorker._campaign_has_recent_reserve = original_reserve

    def test_dial_cancel_to_agent_does_not_decrement(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = self.gen_fail_event('CANCEL', call_type='to_agent')
        event['callid'] = 'cancel-agent-1'
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 3)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT status FROM contact_in_campaign WHERE id_campaign = 4'
                ' AND id_contact = 1;'
            )
            # Dial fail to_agent no debe pisar status del contacto
            self.assertNotEqual(cursor_dialer.fetchone()[0], STATUS_TERMINATED)

    def test_dial_exit_abandon_to_pstn_sets_status_and_decrements(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        original_incidence = AverageWorker.handle_incidence_rules
        AverageWorker.handle_incidence_rules = MagicMock()
        try:
            event = self.gen_fail_event('EXIT_ABANDON', call_type='to_pstn')
            event['callid'] = 'exit-abandon-1'
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding="UTF8"),
            )
            AverageWorker.process_event(self.worker, job)
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 2)
            counter = AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:COUNTER', 'EXIT_ABANDON')
            self.assertEqual(int(counter), 1)
            with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'SELECT status FROM contact_in_campaign WHERE id_campaign = 4'
                    ' AND id_contact = 1;'
                )
                self.assertEqual(cursor_dialer.fetchone()[0], STATUS_EXIT_ABANDON)
            AverageWorker.handle_incidence_rules.assert_not_called()
        finally:
            AverageWorker.handle_incidence_rules = original_incidence

    def test_dial_exit_timeout_to_pstn_sets_status_and_decrements(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        original_incidence = AverageWorker.handle_incidence_rules
        AverageWorker.handle_incidence_rules = MagicMock()
        try:
            event = self.gen_fail_event('EXIT_TIMEOUT', call_type='to_pstn')
            event['callid'] = 'exit-timeout-1'
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding="UTF8"),
            )
            AverageWorker.process_event(self.worker, job)
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 4)
            counter = AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:COUNTER', 'EXIT_TIMEOUT')
            self.assertEqual(int(counter), 1)
            with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'SELECT status FROM contact_in_campaign WHERE id_campaign = 4'
                    ' AND id_contact = 1;'
                )
                self.assertEqual(cursor_dialer.fetchone()[0], STATUS_EXIT_TIMEOUT)
            AverageWorker.handle_incidence_rules.assert_not_called()
        finally:
            AverageWorker.handle_incidence_rules = original_incidence

    def test_dial_exit_answered_updates_campaign_att_without_decrement(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()

        def _exit_answered_job(duration, callid):
            event = {
                'type': 'Dial',
                'id_campaign': '4',
                'contact_id': '1',
                'phone_number': '6093017590',
                'call_type': 'to_pstn',
                'dialstatus': 'EXIT_ANSWERED',
                'agent_duration': duration,
                'callid': callid,
            }
            return GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding="UTF8"),
            )

        AverageWorker.process_event(self.worker, _exit_answered_job(10.0, 'att-1'))
        AverageWorker.process_event(self.worker, _exit_answered_job(30.0, 'att-2'))

        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 3)
        att_key = 'CAMP:4:ATT'
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(att_key, 'ATT_SUM')),
            40.0,
        )
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.hget(att_key, 'ATT_COUNT')), 2)
        self.assertEqual(float(AverageWorker.REDIS_DIALER_CONNECTION.hget(att_key, 'ATT')), 20.0)
        # AHT = ATT + ACW; ACW still missing → AHT == ATT
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:AHT', 'AHT')),
            20.0,
        )
        self.assertEqual(
            int(AverageWorker.REDIS_DIALER_CONNECTION.hget(
                'CAMP:4:COUNTER', 'SIN_DISPOSICION') or 0),
            2,
        )

    def test_dial_exit_answered_no_incr_sin_disposicion_si_ya_califico(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        try:
            with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'UPDATE contact_in_campaign SET disposition_option = 7 '
                    'WHERE id_campaign = 4 AND id_contact = 1;'
                )
                conn.commit()
            event = {
                'type': 'Dial',
                'id_campaign': '4',
                'contact_id': '1',
                'phone_number': '6093017590',
                'call_type': 'to_pstn',
                'dialstatus': 'EXIT_ANSWERED',
                'agent_duration': 10.0,
                'callid': 'att-calif',
            }
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding="UTF8"),
            )
            AverageWorker.process_event(self.worker, job)
            self.assertEqual(
                int(AverageWorker.REDIS_DIALER_CONNECTION.hget(
                    'CAMP:4:COUNTER', 'SIN_DISPOSICION') or 0),
                0,
            )
        finally:
            with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'UPDATE contact_in_campaign SET disposition_option = -1 '
                    'WHERE id_campaign = 4 AND id_contact = 1;'
                )
                conn.commit()

    def test_dial_answered_pstn_updates_campaign_art_without_decrement(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()

        def _answer_pstn_job(ring_duration, callid):
            event = {
                'type': 'Dial',
                'id_campaign': '4',
                'contact_id': '1',
                'phone_number': '6093017590',
                'call_type': 'to_pstn',
                'dialstatus': 'ANSWER',
                'ring_duration': ring_duration,
                'callid': callid,
            }
            return GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding="UTF8"),
            )

        AverageWorker.process_event(self.worker, _answer_pstn_job(5.0, 'art-1'))
        AverageWorker.process_event(self.worker, _answer_pstn_job(15.0, 'art-2'))

        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 3)
        art_key = 'CAMP:4:ART'
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(art_key, 'ART_SUM')),
            20.0,
        )
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.hget(art_key, 'ART_COUNT')), 2)
        self.assertEqual(float(AverageWorker.REDIS_DIALER_CONNECTION.hget(art_key, 'ART')), 10.0)

    def test_dial_answered_pstn_without_ring_duration_skips_art(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 2)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '1',
            'phone_number': '6093017590',
            'call_type': 'to_pstn',
            'dialstatus': 'ANSWER',
            'callid': 'art-no-ring',
        }
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)

        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 2)
        self.assertFalse(AverageWorker.REDIS_DIALER_CONNECTION.exists('CAMP:4:ART'))
        self.assertEqual(
            int(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:COUNTER', 'ANSWERED_PSTN') or 0),
            1,
        )

    def test_dial_exit_acw_updates_campaign_acw_without_decrement(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()

        def _exit_acw_job(duration, callid):
            event = {
                'type': 'Dial',
                'id_campaign': '4',
                'contact_id': '0',
                'phone_number': '0',
                'call_type': 'to_pstn',
                'dialstatus': 'EXIT_ACW',
                'acw_duration': duration,
                'callid': callid,
            }
            return GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding="UTF8"),
            )

        AverageWorker.process_event(self.worker, _exit_acw_job(10.0, 'acw-1'))
        AverageWorker.process_event(self.worker, _exit_acw_job(30.0, 'acw-2'))

        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 3)
        acw_key = 'CAMP:4:ACW'
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget(acw_key, 'ACW_SUM')),
            40.0,
        )
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.hget(acw_key, 'ACW_COUNT')), 2)
        self.assertEqual(float(AverageWorker.REDIS_DIALER_CONNECTION.hget(acw_key, 'ACW')), 20.0)
        # AHT = ATT + ACW; ATT still missing → AHT == ACW
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:AHT', 'AHT')),
            20.0,
        )

    def test_out_of_taxonomy_events_do_not_touch_hit_metrics(self):
        """EXIT_ACW / EXIT_ANSWERED / ANSWERED_AGENT are outside Hit/Fail/Abandon."""
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        AverageWorker.update_campaign_hit(4, hit=True)
        AverageWorker.update_campaign_hit(4, hit=False, abandon=False)
        before = AverageWorker.REDIS_DIALER_CONNECTION.hgetall('CAMP:4:METRICS')

        for event in (
            {
                'type': 'Dial', 'id_campaign': '4', 'contact_id': '0', 'phone_number': '0',
                'call_type': 'to_pstn', 'dialstatus': 'EXIT_ACW', 'acw_duration': 5.0,
                'callid': 'otax-acw',
            },
            {
                'type': 'Dial', 'id_campaign': '4', 'contact_id': '0', 'phone_number': '0',
                'call_type': 'to_pstn', 'dialstatus': 'EXIT_ANSWERED', 'talk_time': 12.0,
                'callid': 'otax-att',
            },
            {
                'type': 'Dial', 'id_campaign': '4', 'contact_id': '1', 'phone_number': '6093017590',
                'call_type': 'to_agent', 'dialstatus': 'ANSWER', 'callid': 'otax-agent',
            },
        ):
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding='UTF8'),
            )
            AverageWorker.process_event(self.worker, job)

        after = AverageWorker.REDIS_DIALER_CONNECTION.hgetall('CAMP:4:METRICS')
        self.assertEqual(before, after)
        self.assertEqual(int(after['HIT_COUNT']), 1)
        self.assertEqual(int(after['FAIL_COUNT']), 1)
        self.assertEqual(int(after.get('ABANDON_COUNT') or 0), 0)

    def test_campaign_aht_is_att_plus_acw(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.GM_CLIENT.submit_job = MagicMock()

        exit_answered = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '1',
            'phone_number': '6093017590',
            'call_type': 'to_pstn',
            'dialstatus': 'EXIT_ANSWERED',
            'agent_duration': 40.0,
            'callid': 'aht-att',
        }
        exit_acw = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '0',
            'phone_number': '0',
            'call_type': 'to_pstn',
            'dialstatus': 'EXIT_ACW',
            'acw_duration': 10.0,
            'callid': 'aht-acw',
        }
        AverageWorker.process_event(
            self.worker,
            GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(exit_answered), encoding="UTF8"),
            ),
        )
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:AHT', 'AHT')),
            40.0,
        )
        AverageWorker.process_event(
            self.worker,
            GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(exit_acw), encoding="UTF8"),
            ),
        )
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:ATT', 'ATT')),
            40.0,
        )
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:ACW', 'ACW')),
            10.0,
        )
        self.assertEqual(
            float(AverageWorker.REDIS_DIALER_CONNECTION.hget('CAMP:4:AHT', 'AHT')),
            50.0,
        )

    def test_dial_exit_acw_without_duration_skips_acw(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 2)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '0',
            'phone_number': '0',
            'call_type': 'to_pstn',
            'dialstatus': 'EXIT_ACW',
            'callid': 'acw-no-dur',
        }
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)

        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 2)
        self.assertFalse(AverageWorker.REDIS_DIALER_CONNECTION.exists('CAMP:4:ACW'))

    def test_dial_cancel_to_pstn_decrements(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = self.gen_fail_event('CANCEL', call_type='to_pstn')
        event['callid'] = 'cancel-pstn-1'
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 2)

    def test_dial_invalid_number_to_pstn_decrements(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = self.gen_fail_event('INVALID_NUMBER', call_type='to_pstn')
        event['callid'] = 'invalid-pstn-1'
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 2)

    def test_dial_chanunavail_to_pstn_decrements(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = self.gen_fail_event('CHANUNAVAIL', call_type='to_pstn')
        event['callid'] = 'chanunavail-pstn-1'
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 2)

    def test_dial_chanunavail_to_agent_does_not_decrement(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        event = self.gen_fail_event('CHANUNAVAIL', call_type='to_agent')
        event['callid'] = 'chanunavail-agent-1'
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(event), encoding="UTF8"),
        )
        AverageWorker.process_event(self.worker, job)
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 3)

    def _set_campaign_active(self, camp_id=4, max_channels=10):
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
            conn.cursor().execute(
                'UPDATE campaign SET dialer_status = %s, max_channels = %s WHERE id = %s',
                (ACTIVE, max_channels, camp_id),
            )
        AverageWorker.get_campaign_max_available_channels.cache_clear()

    def test_schedule_contact_skips_when_at_max_channels(self):
        self._set_campaign_active(4, max_channels=3)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        original_trigger = SchedulerWorker.trigger_acd_dial
        SchedulerWorker.trigger_acd_dial = MagicMock(return_value=True)
        try:
            result = SchedulerWorker.schedule_contact('6093017590', 4, 1)

            self.assertEqual(result, 'Skipped: no free dialer channels')
            SchedulerWorker.trigger_acd_dial.assert_not_called()
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 3)
            with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT status FROM contact_in_campaign '
                    'WHERE id_campaign = 4 AND id_contact = 1'
                )
                status = cursor.fetchone()[0]
            self.assertEqual(status, STATUS_CREATED)
        finally:
            SchedulerWorker.trigger_acd_dial = original_trigger

    def test_schedule_contact_reserves_channel_when_capacity(self):
        self._set_campaign_active(4, max_channels=3)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 1)
        original_trigger = SchedulerWorker.trigger_acd_dial
        SchedulerWorker.trigger_acd_dial = MagicMock(return_value=True)
        try:
            result = SchedulerWorker.schedule_contact('6093017590', 4, 1)

            self.assertEqual(result, 'GD!!!')
            SchedulerWorker.trigger_acd_dial.assert_called_once_with(
                phone_number='6093017590',
                campaign_id=4,
                contact_id=1,
            )
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 2)
            phases = AverageWorker.get_campaign_channel_phases(4)
            self.assertEqual(phases[PHASE_RINGING], 1)
        finally:
            SchedulerWorker.trigger_acd_dial = original_trigger

    def test_schedule_contact_rolls_back_on_gearman_failure(self):
        self._set_campaign_active(4, max_channels=3)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 1)
        original_trigger = SchedulerWorker.trigger_acd_dial
        SchedulerWorker.trigger_acd_dial = MagicMock(return_value=False)
        try:
            result = SchedulerWorker.schedule_contact('6093017590', 4, 1)

            self.assertEqual(result, 'Error sending dial job')
            val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
            self.assertEqual(int(val), 1)
            with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT status FROM contact_in_campaign '
                    'WHERE id_campaign = 4 AND id_contact = 1'
                )
                status = cursor.fetchone()[0]
            self.assertEqual(status, STATUS_CREATED)
        finally:
            SchedulerWorker.trigger_acd_dial = original_trigger

    def test_app_registers_audit_active_channels_job(self):
        # No importar app.py (llama gm_worker.work() a nivel de módulo)
        app_path = os.path.join(os.path.dirname(__file__), 'app.py')
        with open(app_path, encoding='utf-8') as fh:
            content = fh.read()
        self.assertIn(
            "'audit-active-channels': WORKER.audit_active_channels_job",
            content,
        )
        self.assertIn('start_periodic_jobs()', content)
        self.assertIn("'schedule-agenda' in GEARMAN_JOBS", content)

    def test_start_periodic_jobs_registers_audit_enqueue_without_schedule_agenda(self):
        naive_mod._AUDIT_JOB_REGISTERED = False
        mock_scheduler = MagicMock()
        mock_scheduler.running = True
        original_scheduler = SchedulerWorker.SCHEDULER
        SchedulerWorker.SCHEDULER = mock_scheduler
        try:
            SchedulerWorker.start_periodic_jobs()
            mock_scheduler.add_job.assert_called()
            call_args = mock_scheduler.add_job.call_args
            scheduled_fn = call_args.args[0]
            self.assertEqual(
                getattr(scheduled_fn, '__func__', scheduled_fn),
                SchedulerWorker._enqueue_audit_active_channels.__func__,
            )
            self.assertEqual(call_args.kwargs.get('id'), 'audit_dialer_channels')
            self.assertTrue(call_args.kwargs.get('coalesce'))
            self.assertEqual(call_args.kwargs.get('max_instances'), 1)
            self.assertTrue(call_args.kwargs.get('replace_existing'))
            self.assertTrue(naive_mod._AUDIT_JOB_REGISTERED)
        finally:
            SchedulerWorker.SCHEDULER = original_scheduler
            naive_mod._AUDIT_JOB_REGISTERED = False

    def test_enqueue_audit_active_channels_submits_gearman_job(self):
        mock_client = MagicMock()
        original_get = SchedulerWorker._get_gearman_client
        SchedulerWorker._get_gearman_client = MagicMock(return_value=mock_client)
        try:
            SchedulerWorker._enqueue_audit_active_channels()
            mock_client.submit_job.assert_called_once_with(
                AUDIT_ACTIVE_CHANNELS_JOB,
                b'{}',
                background=True,
            )
        finally:
            SchedulerWorker._get_gearman_client = original_get

    def test_audit_lock_acquire_and_release_own_token(self):
        AverageWorker.connect_redis_dialer()
        token = AverageWorker._acquire_audit_lock(ttl_sec=30)
        self.assertIsNotNone(token)
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.get(AUDIT_LOCK_KEY),
            token,
        )
        # Segundo acquire falla mientras el lock vive
        self.assertIsNone(AverageWorker._acquire_audit_lock(ttl_sec=30))
        # Token ajeno no libera
        self.assertFalse(AverageWorker._release_audit_lock('other-token'))
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.get(AUDIT_LOCK_KEY),
            token,
        )
        self.assertTrue(AverageWorker._release_audit_lock(token))
        self.assertIsNone(
            AverageWorker.REDIS_DIALER_CONNECTION.get(AUDIT_LOCK_KEY),
        )

    def test_audit_active_channels_job_runs_with_lock(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        original_audit = AverageWorker.audit_active_channels
        AverageWorker.audit_active_channels = MagicMock()
        job = GearmanJob(
            None, None, b'audit-active-channels',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{}',
        )
        try:
            result = AverageWorker.audit_active_channels_job(self.worker, job)
            self.assertEqual(result, b'Audit completed')
            AverageWorker.audit_active_channels.assert_called_once()
            self.assertIsNone(
                AverageWorker.REDIS_DIALER_CONNECTION.get(AUDIT_LOCK_KEY),
            )
        finally:
            AverageWorker.audit_active_channels = original_audit

    def test_audit_active_channels_job_skips_when_lock_busy(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set(AUDIT_LOCK_KEY, 'busy', ex=30)
        original_audit = AverageWorker.audit_active_channels
        AverageWorker.audit_active_channels = MagicMock()
        job = GearmanJob(
            None, None, b'audit-active-channels',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            b'{}',
        )
        try:
            result = AverageWorker.audit_active_channels_job(self.worker, job)
            self.assertEqual(result, b'Audit skipped: lock busy')
            AverageWorker.audit_active_channels.assert_not_called()
        finally:
            AverageWorker.audit_active_channels = original_audit

    def test_reserve_marks_ringing_and_phase_key(self):
        self._set_campaign_active(4, max_channels=5)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 0)
        AverageWorker._init_campaign_channel_phases(4)
        self.assertTrue(AverageWorker._reserve_dialer_channel(4, 1))
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 1)
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases[PHASE_RINGING], 1)
        self.assertEqual(phases[PHASE_WAITING_AGENT], 0)
        self.assertEqual(phases[PHASE_ONCALL], 0)
        phase = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:PHASE:4:1')
        self.assertEqual(phase, PHASE_RINGING)

    def test_reserve_at_max_does_not_leave_orphan_phase(self):
        self._set_campaign_active(4, max_channels=1)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 1)
        AverageWorker._init_campaign_channel_phases(4)
        AverageWorker.REDIS_DIALER_CONNECTION.hset('CAMP:4:CHANNELS', PHASE_RINGING, 1)
        self.assertFalse(AverageWorker._reserve_dialer_channel(4, 99))
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 1)
        self.assertIsNone(
            AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:PHASE:4:99'),
        )
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases[PHASE_RINGING], 1)

    def test_phase_transitions_answer_pstn_then_agent(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 0)
        AverageWorker._init_campaign_channel_phases(4)
        self._set_campaign_active(4, max_channels=5)
        self.assertTrue(AverageWorker._reserve_dialer_channel(4, 1))

        answer_pstn = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '1',
            'phone_number': '6093017590',
            'call_type': 'to_pstn',
            'dialstatus': 'ANSWER',
            'callid': 'phase-call-1',
            'ring_duration': 2.5,
        }
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(answer_pstn), encoding='UTF8'),
        )
        AverageWorker.process_event(self.worker, job)
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases[PHASE_RINGING], 0)
        self.assertEqual(phases[PHASE_WAITING_AGENT], 1)
        self.assertEqual(phases[PHASE_ONCALL], 0)
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:PHASE:4:1:phase-call-1'),
            PHASE_WAITING_AGENT,
        )
        self.assertIsNone(
            AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:PHASE:4:1'),
        )

        answer_agent = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '1',
            'phone_number': '6093017590',
            'call_type': 'to_agent',
            'dialstatus': 'ANSWER',
            'callid': 'phase-call-1',
            'dialstring': 'camp_4@omlacd',
        }
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(answer_agent), encoding='UTF8'),
        )
        AverageWorker.process_event(self.worker, job)
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases[PHASE_RINGING], 0)
        self.assertEqual(phases[PHASE_WAITING_AGENT], 0)
        self.assertEqual(phases[PHASE_ONCALL], 1)
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 1)

    def test_terminal_decrements_correct_phase_buckets(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        original_incidence = AverageWorker.handle_incidence_rules
        AverageWorker.handle_incidence_rules = MagicMock()
        try:
            # NOANSWER from RINGING
            AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 0)
            AverageWorker._init_campaign_channel_phases(4)
            self._set_campaign_active(4, max_channels=10)
            AverageWorker._reserve_dialer_channel(4, 1)
            event = self.gen_fail_event('NOANSWER', call_type='to_pstn')
            event['callid'] = 'term-noanswer-1'
            event['contact_id'] = '1'
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding='UTF8'),
            )
            AverageWorker.process_event(self.worker, job)
            phases = AverageWorker.get_campaign_channel_phases(4)
            self.assertEqual(phases[PHASE_RINGING], 0)
            self.assertEqual(
                int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')),
                0,
            )

            # EXIT_ABANDON from WAITING_AGENT
            AverageWorker._reserve_dialer_channel(4, 1)
            AverageWorker._transition_channel_phase(
                4, 1, 'term-abandon-1', PHASE_WAITING_AGENT,
            )
            event = self.gen_fail_event('EXIT_ABANDON', call_type='to_pstn')
            event['callid'] = 'term-abandon-1'
            event['contact_id'] = '1'
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(event), encoding='UTF8'),
            )
            AverageWorker.process_event(self.worker, job)
            phases = AverageWorker.get_campaign_channel_phases(4)
            self.assertEqual(phases[PHASE_WAITING_AGENT], 0)
            self.assertEqual(
                int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')),
                0,
            )

            # ChannelDestroyed from ONCALL
            AverageWorker._reserve_dialer_channel(4, 1)
            AverageWorker._transition_channel_phase(4, 1, 'term-oncall-1', PHASE_ONCALL)
            destroy = {
                'type': 'ChannelDestroyed',
                'call_type': 'to_pstn',
                'id_campaign': '4',
                'contact_id': '1',
                'phone_number': '6093017590',
                'callid': 'term-oncall-1',
            }
            job = GearmanJob(
                None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
                bytes(json.dumps(destroy), encoding='UTF8'),
            )
            AverageWorker.process_event(self.worker, job)
            phases = AverageWorker.get_campaign_channel_phases(4)
            self.assertEqual(phases[PHASE_ONCALL], 0)
            self.assertEqual(
                int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')),
                0,
            )
        finally:
            AverageWorker.handle_incidence_rules = original_incidence

    def test_decrement_phase_dedup_and_orphan(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker._init_campaign_channel_phases(4)
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 2)
        AverageWorker.REDIS_DIALER_CONNECTION.hset('CAMP:4:CHANNELS', PHASE_ONCALL, 1)
        AverageWorker.REDIS_DIALER_CONNECTION.set(
            'OML:CALLS:PHASE:4:1:dup-call', PHASE_ONCALL, ex=3600,
        )
        AverageWorker._decrement_calls_once(4, 1, 'dup-call', context='first')
        AverageWorker._decrement_calls_once(4, 1, 'dup-call', context='dup')
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 1)
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases[PHASE_ONCALL], 0)

        # orphan terminal (sin phase key): solo total
        AverageWorker._decrement_calls_once(4, 2, 'orphan-1', context='orphan')
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 0)

    def test_out_of_order_answer_agent_adopts_oncall(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.GM_CLIENT.submit_job = MagicMock()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 1)
        AverageWorker._init_campaign_channel_phases(4)
        answer_agent = {
            'type': 'Dial',
            'id_campaign': '4',
            'contact_id': '1',
            'phone_number': '6093017590',
            'call_type': 'to_agent',
            'dialstatus': 'ANSWER',
            'callid': 'ooo-1',
            'dialstring': 'camp_4@omlacd',
        }
        job = GearmanJob(
            None, None, b'process-event', bytes(str(uuid.uuid4()), encoding='utf8'),
            bytes(json.dumps(answer_agent), encoding='UTF8'),
        )
        AverageWorker.process_event(self.worker, job)
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases[PHASE_ONCALL], 1)
        self.assertEqual(
            AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:PHASE:4:1:ooo-1'),
            PHASE_ONCALL,
        )

    def test_reset_clears_channel_phases(self):
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 3)
        AverageWorker._init_campaign_channel_phases(4)
        AverageWorker.REDIS_DIALER_CONNECTION.hset(
            'CAMP:4:CHANNELS',
            mapping={PHASE_RINGING: 1, PHASE_WAITING_AGENT: 1, PHASE_ONCALL: 1},
        )
        AverageWorker.REDIS_DIALER_CONNECTION.set(
            'OML:CALLS:PHASE:4:1:x', PHASE_ONCALL, ex=3600,
        )
        AverageWorker.reset_dialer_calls_counter(4, reason='test-phases')
        self.assertEqual(int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 0)
        phases = AverageWorker.get_campaign_channel_phases(4)
        self.assertEqual(phases['TOTAL'], 0)
        self.assertIsNone(
            AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:PHASE:4:1:x'),
        )

    def test_audit_reconciles_ringing_with_new_payload(self):
        self._set_campaign_active(4)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 5)
        AverageWorker._init_campaign_channel_phases(4)
        AverageWorker.REDIS_DIALER_CONNECTION.hset('CAMP:4:CHANNELS', PHASE_RINGING, 4)
        AverageWorker.REDIS_DIALER_CONNECTION.hset(
            'CAMP:4:CHANNELS', PHASE_WAITING_AGENT, 1,
        )
        original_fetch = AverageWorker._fetch_asterisk_dialer_channel_counts
        original_reserve = AverageWorker._campaign_has_recent_reserve
        AverageWorker._campaign_has_recent_reserve = MagicMock(return_value=False)
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(
            return_value=(True, {4: 3}, {4: 2})
        )
        try:
            AverageWorker.audit_active_channels()
            self.assertEqual(
                int(AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')), 3,
            )
            phases = AverageWorker.get_campaign_channel_phases(4)
            self.assertEqual(phases[PHASE_RINGING], 2)
        finally:
            AverageWorker._fetch_asterisk_dialer_channel_counts = original_fetch
            AverageWorker._campaign_has_recent_reserve = original_reserve

    def test_audit_skips_ringing_reconcile_without_field(self):
        self._set_campaign_active(4)
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.set('OML:CALLS:4:DIALER', 2)
        AverageWorker._init_campaign_channel_phases(4)
        AverageWorker.REDIS_DIALER_CONNECTION.hset('CAMP:4:CHANNELS', PHASE_RINGING, 2)
        original_fetch = AverageWorker._fetch_asterisk_dialer_channel_counts
        original_reserve = AverageWorker._campaign_has_recent_reserve
        AverageWorker._campaign_has_recent_reserve = MagicMock(return_value=False)
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(
            return_value=(True, {4: 2}, None)
        )
        try:
            AverageWorker.audit_active_channels()
            phases = AverageWorker.get_campaign_channel_phases(4)
            self.assertEqual(phases[PHASE_RINGING], 2)
        finally:
            AverageWorker._fetch_asterisk_dialer_channel_counts = original_fetch
            AverageWorker._campaign_has_recent_reserve = original_reserve

    def _make_oml_cursor_for_shuffle(self, shuffle):
        cursor_oml = MagicMock()
        cursor_oml.fetchone.return_value = (shuffle,)
        executed = []

        def track_execute(sql, params=None):
            executed.append(sql)

        cursor_oml.execute = track_execute
        return cursor_oml, executed

    def mocked_get_contacts_campaign_shuffled(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            # Deliberately out of ascending id order to assert insert order.
            return [
                (2, '5143016455',
                 '["Ashley Barrett", "Edward Townsend", "8718745", "5618936401", "1075763364"]',
                 True),
                (1, '6093017590',
                 '["Amanda Jenkins", "Gregory Henson", "7147034", "4067530816", "5273724517"]',
                 True),
            ]
        return []

    def mocked_get_contacts_campaign_many(self, cursor, size):
        if self.fetchmany_counter == 0:
            self.fetchmany_counter += 1
            # 100 contacts in descending id order (simulates ORDER BY random outcome).
            return [
                (i, f'5{i:09d}', f'["contact_{i}"]', True)
                for i in range(100, 0, -1)
            ]
        return []

    def test_copy_contacts_from_oml_uses_order_by_random_when_shuffle_enabled(self):
        cursor_oml, executed = self._make_oml_cursor_for_shuffle(True)
        self.fetchmany_counter = 0
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'DELETE FROM contact_in_campaign WHERE id_campaign = %s;', (4,))
                AverageWorker.copy_contacts_from_oml(cursor_dialer, cursor_oml, 4)
        contacts_sql = [s for s in executed if 'ominicontacto_app_contacto' in s][0]
        self.assertIn('ORDER BY random()', contacts_sql)

    def test_copy_contacts_from_oml_no_order_by_random_when_shuffle_disabled(self):
        cursor_oml, executed = self._make_oml_cursor_for_shuffle(False)
        self.fetchmany_counter = 0
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'DELETE FROM contact_in_campaign WHERE id_campaign = %s;', (4,))
                AverageWorker.copy_contacts_from_oml(cursor_dialer, cursor_oml, 4)
        contacts_sql = [s for s in executed if 'ominicontacto_app_contacto' in s][0]
        self.assertNotIn('ORDER BY random()', contacts_sql)

    def test_copy_contacts_from_oml_preserves_fetch_order_on_insert(self):
        cursor_oml, _ = self._make_oml_cursor_for_shuffle(True)
        self.fetchmany_counter = 0
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign_shuffled)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'DELETE FROM contact_in_campaign WHERE id_campaign = %s;', (4,))
                cursor_dialer.execute('DELETE FROM contact WHERE id IN (1, 2);')
                AverageWorker.copy_contacts_from_oml(cursor_dialer, cursor_oml, 4)
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT id_contact FROM contact_in_campaign WHERE id_campaign = %s '
                'ORDER BY id;', (4,))
            ordered_contacts = [row[0] for row in cursor_dialer.fetchall()]
        self.assertEqual(ordered_contacts, [2, 1])

    def test_copy_contacts_from_oml_shuffle_keeps_full_set(self):
        cursor_oml, executed = self._make_oml_cursor_for_shuffle(True)
        self.fetchmany_counter = 0
        AverageWorker.get_contacts_campaign = MagicMock(
            side_effect=self.mocked_get_contacts_campaign_many)
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'DELETE FROM contact_in_campaign WHERE id_campaign = %s;', (4,))
                cursor_dialer.execute(
                    'DELETE FROM contact WHERE id BETWEEN 1 AND 100;')
                AverageWorker.copy_contacts_from_oml(cursor_dialer, cursor_oml, 4)
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT id_contact FROM contact_in_campaign WHERE id_campaign = %s '
                'ORDER BY id;', (4,))
            ordered_contacts = [row[0] for row in cursor_dialer.fetchall()]
        self.assertEqual(set(ordered_contacts), set(range(1, 101)))
        self.assertEqual(len(ordered_contacts), 100)
        # Insertion follows fetch order (descending ids), not ascending contact id.
        self.assertEqual(ordered_contacts, list(range(100, 0, -1)))
        contacts_sql = [s for s in executed if 'ominicontacto_app_contacto' in s][0]
        self.assertIn('ORDER BY random()', contacts_sql)


if __name__ == '__main__':
    unittest.main()
