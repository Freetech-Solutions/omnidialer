# -*- coding: utf-8 -*-

from flask import Flask, request, render_template, jsonify

from dialer.multichannel import GearmanDialer

from settings.default import WEBSOCKET_SERVER

SYNC_OMNILEADS_TRUE_VALUES = {'1', 'true', 't', 'yes', 'y', 'on'}


def _normalize_sync_omnileads(value):
    """Normalize sync-omnileads parameter values to a boolean."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='ignore')
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in SYNC_OMNILEADS_TRUE_VALUES
    return bool(value)


app = Flask(__name__)

DIALER = GearmanDialer

# TODO: pass a parameter called 'type' for dispatch to
# the class linked to that kind of a campaing (voip, email, Whatsapp, Telegram, SMS, etc)


@app.route('/create-campaign/<id_campaign>', methods=['POST'])
def create_campaign(id_campaign):
    strategy = request.get_json().get('contact-strategy', [])
    prefix = request.get_json().get('prefix')
    if prefix == []:
        prefix = None
    return DIALER.create_campaign(id_campaign, strategy, prefix)

@app.route('/edit-campaign/<id_campaign>', methods=['POST'])
def edit_campaign(id_campaign):
    strategy = request.get_json().get('contact-strategy', [])
    return DIALER.edit_campaign(id_campaign, strategy)


@app.route('/start-campaign/<id_campaign>', methods=['POST'])
def start_campaign(id_campaign):
    sync_omnileads = _normalize_sync_omnileads(request.form.get('sync-omnileads'))
    return DIALER.start_campaign(id_campaign, sync_omnileads=sync_omnileads)


@app.route('/stop-campaign/<id_campaign>', methods=['POST'])
def stop_campaign(id_campaign):
    sync_omnileads = _normalize_sync_omnileads(request.form.get('sync-omnileads'))
    return DIALER.stop_campaign(id_campaign, sync_omnileads=sync_omnileads)


@app.route('/pause-campaign/<id_campaign>', methods=['POST'])
def pause_campaign(id_campaign):
    sync_omnileads = _normalize_sync_omnileads(request.form.get('sync-omnileads'))
    return DIALER.pause_campaign(id_campaign, sync_omnileads=sync_omnileads)


@app.route('/resume-campaign/<id_campaign>', methods=['POST'])
def resume_campaign(id_campaign):
    return DIALER.resume_campaign(id_campaign)


@app.route('/delete-campaign/<id_campaign>', methods=['POST'])
def delete_campaign(id_campaign):
    return DIALER.delete_campaign(id_campaign)


@app.route('/add-incidence-rule-disposition/<id_campaign>', methods=['POST'])
def add_incidence_rule_disposition(id_campaign):
    id_contact = request.get_json().get('id_contact', -1)
    disposition_option = request.get_json().get('disposition_option', -1)
    return DIALER.add_incidence_rule_disposition(id_campaign, disposition_option, id_contact)


@app.route('/create-incidence-rule/<id_campaign>', methods=['POST'])
def create_incidence_rule(id_campaign):
    json_value = request.get_json()
    id_rule = json_value.get('id', -1)
    type_rule = json_value.get('type', -1)
    status = json_value.get('status', -1)
    status_custom = json_value.get('status_custom', "")
    disposition_option_id = json_value.get('disposition_option_id', -1)
    max_attempt = json_value.get('max_attempt', -1)
    retry_later = json_value.get('retry_later', -1)
    mode = json_value.get('in_mode', -1)
    return DIALER.create_incidence_rule(
        id_campaign, id_rule, status, status_custom, max_attempt,
        retry_later, mode, disposition_option_id, type_rule
    )


@app.route('/delete-incidence-rule/<id_campaign>', methods=['POST'])
def delete_incidence_rule(id_campaign):
    json_value = request.get_json()
    id_rule = json_value.get('id')
    type_rule = json_value.get('type')
    return DIALER.delete_incidence_rule(
        id_campaign, id_rule, type_rule
    )


@app.route('/update-incidence-rule/<id_campaign>', methods=['POST'])
def update_incidence_rule(id_campaign):
    json_value = request.get_json()
    id_rule = json_value.get('id', -1)
    type_rule = json_value.get('type', -1)
    status = json_value.get('status', -1)
    status_custom = json_value.get('status_custom', "")
    disposition_option_id = json_value.get('disposition_option_id', -1)
    max_attempt = json_value.get('max_attempt', -1)
    retry_later = json_value.get('retry_later', -1)
    mode = json_value.get('in_mode', -1)
    return DIALER.update_incidence_rule(
        id_campaign, id_rule, status, status_custom, max_attempt,
        retry_later, mode, disposition_option_id, type_rule
    )


@app.route('/add-agenda/<id_campaign>', methods=['POST'])
def add_agenda(id_campaign):
    datetime_agenda = request.get_json().get('datetime', '')
    campaign_name = request.get_json().get('campaign_name', '')
    phone_number = request.get_json().get('phone_number', '')
    id_contact = request.get_json().get('id_contact', '')
    return DIALER.add_agenda(id_campaign, id_contact, campaign_name, datetime_agenda, phone_number)


@app.route('/change-database/<id_campaign>', methods=['POST'])
def change_database(id_campaign):
    return DIALER.change_database(id_campaign)


@app.route('/add_amd_event', methods=['POST'])
def add_amd_event():
    data = request.get_json()
    return DIALER.add_amd_event(data)

@app.route('/external-manual-call', methods=['POST'])
def external_manual_call():
    data = request.get_json(silent=True) or {}
    phone_number = data.get("phone_number")
    id_agent = data.get("id_agent")

    if not phone_number:
        return jsonify({"error": "phone_number is required"}), 400
    if id_agent is None:
        return jsonify({"error": "id_agent is required"}), 400

    result = DIALER.external_manual_call(
        phone_number=phone_number,
        id_agent=id_agent,
    )

    return jsonify({
        "status": "queued",
        "id_agent": id_agent,
        "phone_number": phone_number,
        "job": result,
    }), 202

@app.route('/agent2agent', methods=['POST'])
def agent2agent():
    data = request.get_json(silent=True) or {}
    id_agent_origen = data.get("id_agent_origen")
    id_agent_destino = data.get("id_agent_destino")

    if id_agent_origen is None:
        return jsonify({"error": "id_agent_origen is required"}), 400
    if id_agent_destino is None:
        return jsonify({"error": "id_agent_destino is required"}), 400

    result = DIALER.agent2agent_call(
        id_agent_origen,
        id_agent_destino
    )

    return jsonify({
        "status": "queued",
        "id_agent_origen": id_agent_origen,
        "id_agent_destino": id_agent_destino,
        "job": result,
    }), 202

@app.route('/manual-call/<int:id_campaign>', methods=['POST'])
def manual_call(id_campaign):
    data = request.get_json(silent=True) or {}
    phone_number = data.get("phone_number")
    id_contact = data.get("id_contact")
    id_agent = data.get("id_agent")

    if not phone_number:
        return jsonify({"error": "phone_number is required"}), 400
    if id_agent is None:
        return jsonify({"error": "id_agent is required"}), 400

    result = DIALER.manual_call(
        id_campaign=id_campaign,
        id_contact=id_contact,
        phone_number=phone_number,
        id_agent=id_agent,
    )

    return jsonify({
        "status": "queued",
        "id_campaign": id_campaign,
        "id_contact": id_contact,
        "id_agent": id_agent,
        "phone_number": phone_number,
        "job": result,
    }), 202


@app.route('/call-campaign-contact/<int:id_campaign>', methods=['POST'])
def call_campaign_contact(id_campaign):
    data = request.get_json(silent=True) or {}

    id_contact = data.get("id_contact")
    id_agent = data.get("id_agent")

    force = bool(data.get("force", False))
    ignore_opening_hours = bool(data.get("ignore_opening_hours", False))

    if id_agent is None:
        return jsonify({"error": "id_agent is required"}), 400

    result = DIALER.call_campaign_contact(
        id_campaign=id_campaign,
        id_contact=id_contact,
        id_agent=id_agent,
        force=force,
        ignore_opening_hours=ignore_opening_hours
    )

    return jsonify({
        "status": "queued",
        "id_campaign": id_campaign,
        "id_contact": id_contact,
        "id_agent": id_agent,
        "force": force,
        "ignore_opening_hours": ignore_opening_hours,
        "job": result
    }), 202


# HTMX endpoints & UI related code
app.jinja_env.globals['WEBSOCKET_SERVER'] = WEBSOCKET_SERVER


# TODO: move this endpoint to the workers
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/htmx/init')
def init():
    """Renders the initial data of the campaigns, on the first load of the web page"""
    return DIALER.render_template({'type': 'init'})


@app.route('/htmx/stats/<id_campaign>')
def stats(id_campaign):
    """Renders the initial data of the campaigns, on the first load of the web page"""
    return DIALER.render_template(
        {'type': 'stats', 'id_campaign': id_campaign}
    )


@app.route('/htmx/manage-dialer/', methods=['POST'])
def manage_dialer():
    action = request.form.get('action')
    return DIALER.manage_dialer(action)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1440, debug=True)
