# -*- coding: utf-8 -*-


import gearman.client

import json
import logging

from .basic import Dialer

from settings.default import GEARMAN_JOB_SERVERS


logger = logging.getLogger(__name__)


class GearmanDialer(Dialer):
    """A dialer design to make multi-channel contacts using Gearman for horizontal scalability"""

    GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)

    @classmethod
    def encode_payload(cls, data):
        return bytes(json.dumps(data), encoding="UTF8")

    @classmethod
    def decode_payload(cls, data):
        return data.decode(encoding="utf8")

    @classmethod
    def decode_job_response(cls, job_request, task_name):
        if job_request.result is None:
            logger.error(
                "Gearman job '%s' returned no result. state=%s timed_out=%s exception=%r",
                task_name,
                job_request.state,
                job_request.timed_out,
                job_request.exception,
            )
            if job_request.exception:
                # raise RuntimeError(
                msg = f"Gearman job '{task_name}' failed: {job_request.exception}"
            if job_request.timed_out:
                # raise TimeoutError(
                msg = f"Gearman job '{task_name}' timed out"
            # raise RuntimeError(
                msg = f"Gearman job '{task_name}' returned no result (state={job_request.state})"
            return {'gearman_error': msg}
        return cls.decode_payload(job_request.result)

    @classmethod
    def create_campaign(cls, id_campaign, contact_strategy, prefix):
        payload = {
            'id_campaign': id_campaign,
            'contact_strategy': contact_strategy,
            'prefix': prefix
        }
        payload_bytes = cls.encode_payload(payload)
        job_request = cls.GM_CLIENT.submit_job(
            'create-campaign',
            payload_bytes)
        return cls.decode_job_response(job_request, 'create-campaign')

    @classmethod
    def edit_campaign(cls, id_campaign, contact_strategy):
        payload = {
            'id_campaign': id_campaign,
            'contact_strategy': contact_strategy
        }
        payload_bytes = cls.encode_payload(payload)
        job_request = cls.GM_CLIENT.submit_job(
            'edit-campaign',
            payload_bytes)
        return cls.decode_job_response(job_request, 'edit-campaign')

    @classmethod
    def start_campaign(cls, id_campaign, sync_omnileads=False):
        payload = {
            'id_campaign': id_campaign,
            'sync_omnileads': sync_omnileads
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('start-campaign', payload_bytes, background=True)
        return json.dumps({'msg': 'Campaign process to be started'})

    @classmethod
    def pause_campaign(cls, id_campaign, sync_omnileads=False):
        payload = {
            'id_campaign': id_campaign,
            'sync_omnileads': sync_omnileads
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('pause-campaign', payload_bytes, background=True)
        return json.dumps({'msg': 'Campaign process to be paused'})

    @classmethod
    def resume_campaign(cls, id_campaign, sync_omnileads=False):
        payload = {
            'id_campaign': id_campaign,
            'sync_omnileads': sync_omnileads
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('resume-campaign', payload_bytes, background=True)
        return json.dumps({'msg': 'Campaign process to be resumed'})

    @classmethod
    def delete_campaign(cls, id_campaign, sync_omnileads=False):
        payload = {
            'id_campaign': id_campaign,
            'sync_omnileads': sync_omnileads
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('delete-campaign', payload_bytes)
        return json.dumps({'msg': 'Campaign deleted'})

    @classmethod
    def stop_campaign(cls, id_campaign, sync_omnileads=False):
        payload = {
            'id_campaign': id_campaign,
            'sync_omnileads': sync_omnileads
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('stop-campaign', payload_bytes)
        return json.dumps({'msg': 'Campaign finalized'})

    @classmethod
    def add_incidence_rule_disposition(cls, id_campaign, disposition_option, id_contact):
        payload = {
            'id_campaign': id_campaign,
            'disposition_option': disposition_option,
            'id_contact': id_contact,
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('add-incidence-rule-disposition', payload_bytes)
        return json.dumps({'msg': 'Disposition added'})

    @classmethod
    def create_incidence_rule(
            cls, id_campaign, id_rule, status, status_custom, max_attempt,
            retry_later, mode, disposition_option_id, type_rule):
        payload = {
            'id_campaign': id_campaign,
            'id_rule': id_rule,
            'status': status,
            'status_custom': status_custom,
            'max_attempt': max_attempt,
            'retry_later': retry_later,
            'mode': mode,
            'disposition_option_id': disposition_option_id,
            'type_rule': type_rule
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('create-incidence-rule', payload_bytes)
        return json.dumps({'msg': 'Incide rule added'})

    @classmethod
    def delete_incidence_rule(cls, id_campaign, id_rule, type_rule):
        payload = {
            'id_campaign': id_campaign,
            'id_rule': id_rule,
            'type_rule': type_rule
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('delete-incidence-rule', payload_bytes)
        return json.dumps({'msg': 'Incidence rule deleted'})

    @classmethod
    def update_incidence_rule(cls, id_campaign, id_rule, status, status_custom, max_attempt,
                              retry_later, mode, disposition_option_id, type_rule):
        payload = {
            'id_campaign': id_campaign,
            'id_rule': id_rule,
            'status': status,
            'status_custom': status_custom,
            'max_attempt': max_attempt,
            'retry_later': retry_later,
            'mode': mode,
            'disposition_option_id': disposition_option_id,
            'type_rule': type_rule
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('update-incidence-rule', payload_bytes)
        return json.dumps({'msg': 'Incidence rule was updated'})

    @classmethod
    def add_agenda(cls, id_campaign, id_contact, campaign_name, datetime_agenda, phone_number):
        payload = {
            'id_campaign': id_campaign,
            'id_contact': id_contact,
            'campaign_name': campaign_name,
            'datetime_agenda': datetime_agenda,
            'phone_number': phone_number,
            'type': 'agenda'
        }
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('schedule-agenda', payload_bytes, background=True)
        return json.dumps({'msg': 'Agenda was sent for scheduling'})

    @classmethod
    def change_database(cls, id_campaign):
        payload = {'id_campaign': id_campaign}
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('change-database', payload_bytes)
        return json.dumps({'msg': 'Database was changed'})

    @classmethod
    def manage_dialer(cls, action):
        payload = {'action': action}
        payload_bytes = cls.encode_payload(payload)
        job_request = cls.GM_CLIENT.submit_job('manage-dialer', payload_bytes)
        return cls.decode_job_response(job_request, 'manage-dialer')

    @classmethod
    def render_template(cls, data):
        job_request = cls.GM_CLIENT.submit_job('render-template', cls.encode_payload(data))
        return cls.decode_job_response(job_request, 'render-template')

    @classmethod
    def add_amd_event(cls, data):
        payload = data
        payload.update([('disposition_option', -2)])
        payload_bytes = cls.encode_payload(payload)
        cls.GM_CLIENT.submit_job('add-incidence-rule-disposition', payload_bytes)
        return json.dumps({'msg': 'Disposition added'})
