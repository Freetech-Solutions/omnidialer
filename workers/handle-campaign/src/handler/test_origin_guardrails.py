# -*- coding: utf-8 -*-
import unittest
from unittest.mock import MagicMock, patch

from handler import origin_guardrails as og


class OriginGuardrailsTests(unittest.TestCase):

    def test_backpressure_process_event_backlog(self):
        with patch.object(og, 'ORIGIN_GUARDRAILS_ENABLED', True), \
             patch.object(og, 'GUARDRAIL_PROCESS_EVENT_QUEUED_MAX', 100), \
             patch.object(og, 'GUARDRAIL_CALLS_AST_DELTA_MAX', 0), \
             patch.object(og, 'GUARDRAIL_PHASE_CALLS_DELTA_MAX', 0), \
             patch.object(og, 'fetch_process_event_queued', return_value=150):
            block, reason = og.evaluate_backpressure_guardrails(
                active_calls=1, phase_total=1, ast_channels=1,
            )
        self.assertTrue(block)
        self.assertEqual(reason, 'process_event_backlog')

    def test_backpressure_calls_ast_delta(self):
        with patch.object(og, 'ORIGIN_GUARDRAILS_ENABLED', True), \
             patch.object(og, 'GUARDRAIL_PROCESS_EVENT_QUEUED_MAX', 0), \
             patch.object(og, 'GUARDRAIL_CALLS_AST_DELTA_MAX', 15), \
             patch.object(og, 'GUARDRAIL_PHASE_CALLS_DELTA_MAX', 0), \
             patch.object(og, 'fetch_process_event_queued', return_value=0):
            block, reason = og.evaluate_backpressure_guardrails(
                active_calls=40, phase_total=40, ast_channels=10,
            )
        self.assertTrue(block)
        self.assertEqual(reason, 'calls_ast_delta')

    def test_backpressure_disabled(self):
        with patch.object(og, 'ORIGIN_GUARDRAILS_ENABLED', False), \
             patch.object(og, 'fetch_process_event_queued', return_value=999):
            block, reason = og.evaluate_backpressure_guardrails(
                active_calls=100, phase_total=200, ast_channels=0,
            )
        self.assertFalse(block)
        self.assertEqual(reason, '')

    def test_global_caps_consume(self):
        r = MagicMock()
        r.incr.side_effect = [1, 2, 3]
        with patch.object(og, 'ORIGIN_GUARDRAILS_ENABLED', True), \
             patch.object(og, 'GLOBAL_CAPS', 2):
            ok1, _ = og.try_consume_global_caps(r)
            ok2, _ = og.try_consume_global_caps(r)
            ok3, reason = og.try_consume_global_caps(r)
        self.assertTrue(ok1)
        self.assertTrue(ok2)
        self.assertFalse(ok3)
        self.assertEqual(reason, 'global_caps')


if __name__ == '__main__':
    unittest.main()
