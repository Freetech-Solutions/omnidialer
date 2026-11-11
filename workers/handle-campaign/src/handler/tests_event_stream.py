"""Smoke tests for dialer event_stream helper."""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch


class EventStreamHelperTests(unittest.TestCase):

    def test_xadd_event_writes_value_field(self):
        from handler.event_stream import STREAM_DIALER, xadd_event

        conn = MagicMock()
        ok = xadd_event(conn, STREAM_DIALER, {'type': 'CALLS', 'camp_id': 1, 'calls': 2}, maxlen=100)
        self.assertTrue(ok)
        args = conn.execute_command.call_args[0]
        self.assertEqual(args[0], 'XADD')
        self.assertEqual(args[1], STREAM_DIALER)
        self.assertEqual(args[-2], 'value')
        self.assertEqual(json.loads(args[-1])['type'], 'CALLS')

    def test_xadd_event_none_conn(self):
        from handler.event_stream import STREAM_DIALER, xadd_event
        self.assertFalse(xadd_event(None, STREAM_DIALER, {'type': 'CREATE'}))

    def test_notify_dialer_event_xadd_only(self):
        from handler import event_stream as es

        stream_conn = MagicMock()
        pub_conn = MagicMock()
        with patch.object(es, 'get_stream_redis', return_value=stream_conn):
            es.notify_dialer_event({'type': 'DELETE', 'camp_id': 9}, publish_conn=pub_conn)
        stream_conn.execute_command.assert_called_once()
        pub_conn.publish.assert_not_called()


if __name__ == '__main__':
    unittest.main()
