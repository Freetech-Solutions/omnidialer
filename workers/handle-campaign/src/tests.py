# -*- coding: utf-8 -*-

import datetime
import unittest
import uuid
from unittest.mock import MagicMock
from decimal import Decimal
import psycopg
import time
import json

from datetime import timedelta
from gearman.job import GearmanJob
from gearman.worker import GearmanWorker

from handler.naive import (
    AverageWorker, ACTIVE, PAUSED, CREATED, FINALIZED, STATUS_SELECTED_CALL,
    STATUS_CREATED, FINALIZED_NOCONTACT, STATUS_AMD_MACHINE
)


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

    def gen_fail_event(self, event):
        return {'type': 'Dial',
                'timestamp': '2025-04-15T11:21:29.168-0300',
                'id_campaign': '4',
                'contact_id': '1',
                'phone_number': '6093017590',
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

    def test_manual_call_blocks_blacklisted(self):
        """Verifica que una llamada manual a un blacklisted sea rechazada"""
        AverageWorker.is_blacklisted = MagicMock(return_value=True)
        AverageWorker.attempt_contact_asterisk = MagicMock()

        job_data = {
            "id_campaign": 4,
            "agent_id": 1,
            "contact": [1, 4, "1234567890"]
        }
        job = GearmanJob(
            None, None, b'manual-call',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            self.encode_payload(job_data)
        )

        result = AverageWorker.manual_call(self.worker, job)

        self.assertEqual(result, b"Error: Number is Blacklisted")
        AverageWorker.attempt_contact_asterisk.assert_not_called()
        del AverageWorker.is_blacklisted

    def test_preview_call_blocks_blacklisted(self):
        """Verifica que una llamada preview (call_campaign_contact) sea rechazada y limpiada"""
        AverageWorker.is_blacklisted = MagicMock(return_value=True)
        AverageWorker.attempt_contact_asterisk = MagicMock()

        # Simulamos que el contacto está en estado SELECTED_CALL (15) listo para llamar
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE contact_in_campaign SET status = %s WHERE id_contact = 1",
                (STATUS_SELECTED_CALL,)
            )

        job_data = {
            "id_campaign": 4,
            "agent_id": 1,
            "id_contact": 1
        }
        job = GearmanJob(
            None, None, b'call-campaign-contact',
            bytes(str(uuid.uuid4()), encoding='utf8'),
            self.encode_payload(job_data)
        )

        result = AverageWorker.call_campaign_contact(self.worker, job)

        self.assertEqual(result, b"Aborted: Number is Blacklisted")
        AverageWorker.attempt_contact_asterisk.assert_not_called()

        # Verificar que el contacto fue devuelto a la cola (schedule_aborted=True)
        # o finalizado, según la lógica que implementamos.
        # En tu implementación pusimos 'schedule_aborted = true' para preview.
        with psycopg.connect(AverageWorker.POSTGRES_DIALER_CONNECTION_STR) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT schedule_aborted FROM contact_in_campaign WHERE id_contact=1"
            )
            val = cur.fetchone()[0]
            self.assertTrue(
                val, "El contacto preview debió marcarse como schedule_aborted=True"
            )

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
        AverageWorker._fetch_asterisk_dialer_channel_counts = MagicMock(return_value={4: 0})
        AverageWorker.audit_active_channels()
        val = AverageWorker.REDIS_DIALER_CONNECTION.get('OML:CALLS:4:DIALER')
        self.assertEqual(int(val), 0)


if __name__ == '__main__':
    unittest.main()
