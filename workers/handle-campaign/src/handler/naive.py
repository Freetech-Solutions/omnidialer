# -*- coding: utf-8 -*-

from .basic import DialerWorker

import atexit
import signal
import threading

from .utils import timed_lru_cache

import re
import json
import os
import redis
from psycopg_pool import ConnectionPool
import gearman
import datetime
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.redis import RedisJobStore

from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_REMOVED,
    EVENT_JOB_MISSED,
)

from datetime import timedelta
from math import floor, ceil
from time import sleep

from settings.default import (REDIS_DIALER_PORT, REDIS_DIALER_SERVER, GEARMAN_JOB_SERVERS,
                              TIME_BETWEEN_CALLS)

import logging

from ui.rendering import AdminRender
# --- logging primero (ANTES de atexit/signal) ---
LOGLEVEL = os.environ.get('PYTHON_LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOGLEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flag global para registrar listener una sola vez
_SCHED_LISTENER_REGISTERED = False
# Flag global para shutdown ordenado
_SCHED_SHUTDOWN_DONE = False


def _shutdown_scheduler_gracefully():
    """
    Cierra el scheduler sin bloquear para evitar warnings al terminar el proceso.

    - Evita NameError si SchedulerWorker todavía no existe (signal temprano durante import).
    """
    global _SCHED_SHUTDOWN_DONE
    if _SCHED_SHUTDOWN_DONE:
        return
    _SCHED_SHUTDOWN_DONE = True

    try:
        sw = globals().get("SchedulerWorker")  # puede no existir aún
        sched = getattr(sw, "SCHEDULER", None) if sw else None
        if sched is None:
            logger.debug("Scheduler shutdown: SchedulerWorker/SCHEDULER no disponible aún.")
            return

        # Verificar si el scheduler está corriendo antes de hacer shutdown
        if sched.running:
            sched.shutdown(wait=False)
            logger.info("Scheduler shutdown solicitado (wait=False)")
        else:
            logger.debug("Scheduler shutdown: scheduler no está corriendo, omitiendo shutdown.")
    except Exception as e:
        logger.debug("Scheduler shutdown: %s", e, exc_info=True)


def _sched_sig_handler(signum, frame):
    """Ejecuta shutdown en un hilo daemon para no bloquear el signal handler."""
    try:
        threading.Thread(target=_shutdown_scheduler_gracefully, daemon=True).start()
    except Exception:
        # último recurso: evitar que un error aquí tumbe el proceso
        try:
            logger.debug("SIG handler: no se pudo iniciar hilo para shutdown.", exc_info=True)
        except Exception:
            pass


# Registrar hooks de salida (hazlo una sola vez por módulo)
atexit.register(_shutdown_scheduler_gracefully)
signal.signal(signal.SIGTERM, _sched_sig_handler)
signal.signal(signal.SIGINT, _sched_sig_handler)


REDIS_OML_SERVER = os.getenv('REDIS_OML_SERVER', 'oml-redis')

REDIS_OML_PORT = os.getenv('REDIS_OML_PORT', '6379')

REDIS_DIALER_DB = int(os.getenv('REDIS_DIALER_DB', 3))

POSTGRES_OML_SERVER = os.getenv('POSTGRES_OML_SERVER', 'oml-postgres')

POSTGRES_OML_PORT = os.getenv('POSTGRES_OML_PORT', '5432')

POSTGRES_OML_PASSWORD = os.getenv('POSTGRES_OML_PASSWORD')

POSTGRES_OML_USER = os.getenv('POSTGRES_OML_USER', 'omnileads')

POSTGRES_OML_DB = os.getenv('POSTGRES_OML_DB', 'omnileads')

POSTGRES_DIALER_SERVER = os.getenv('POSTGRES_DIALER_SERVER', 'dialer-postgres')

POSTGRES_DIALER_PORT = os.getenv('POSTGRES_DIALER_PORT', '5433')

POSTGRES_DIALER_USER = os.getenv('POSTGRES_DIALER_USER', 'omnidialer')

POSTGRES_DIALER_DB = os.getenv('POSTGRES_DIALER_DB', 'omnidialer')

POSTGRES_DIALER_PASSWORD = os.getenv('POSTGRES_DIALER_PASSWORD')

DIALER_ACD_HOST = os.getenv('DIALER_ACD_HOST', 'omlacd')

DIALER_DIALPLAN_CONTEXT = os.getenv('DIALER_DIALPLAN_CONTEXT', 'oml-dial-dialer')

SCHEDULER_API_HOST = os.getenv('SCHEDULER_API_HOST', 'scheduler-api')

WEEK_DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

CAPS = int(os.getenv('CAPS', 3))

# campaign status possible values
CREATED = 1                     # ESTADO_INACTIVA in OML
ACTIVE = 2                      # ESTADO_ACTIVA in OML
PAUSED = 5                      # ESTADO_PAUSADA in OML
FINALIZED = 3                   # ESTADO_FINALIZADA in OML

CAMPAIGN_STATUS_TO_NAME = {
    1: "INACTIVE",
    2: "ACTIVE",
    5: "PAUSED",
    3: "FINALIZED"
}

AVAILABLE_NEXT_STATUSES = {
    CREATED: [ACTIVE, FINALIZED],
    ACTIVE: [PAUSED, FINALIZED],
    PAUSED: [ACTIVE, FINALIZED],
    FINALIZED: [ACTIVE]
}

# contact status, in sync with OML's incidence_rules statuses
# TODO: see the remaining statuses
STATUS_CREATED = 12
STATUS_SELECTED_CALL = 15
STATUS_ANSWERED_AGENT = 6
STATUS_ANSWERED_PSTN = 7
STATUS_BUSY = 1
STATUS_NOANSWER = 3
STATUS_CONGESTION = 4
STATUS_TERMINATED = 2
STATUS_TIMEOUT = 5
STATUS_CHANUNAVAIL = 8
STATUS_INVALID_NUMBER = 9
STATUS_AMD_MACHINE = 10  # AMD declaró MACHINE (contestador); entidad propia para métricas
STATUS_SHORTCALL = 11  # contestó y colgó en <5s; entidad propia para métricas

NAME_TO_STATUS = {
    "CHANUNAVAIL": STATUS_CHANUNAVAIL,
    "INVALID_NUMBER": STATUS_INVALID_NUMBER,
    "BUSY": STATUS_BUSY,
    "NOANSWER": STATUS_NOANSWER,
    "CONGESTION": STATUS_CONGESTION,
    "ANSWERED_PSTN": STATUS_ANSWERED_PSTN,
    "ANSWERED_AGENT": STATUS_ANSWERED_AGENT,
    "TERMINATED": STATUS_TERMINATED,
    "TIMEOUT": STATUS_TIMEOUT,
    "CANCEL": STATUS_TERMINATED,  # llamada cancelada antes de contestar; sin reglas de incidencia
    "AMD": STATUS_AMD_MACHINE,  # contestador detectado; entidad propia en history y métricas
    "EXIT_SHORTCALL": STATUS_SHORTCALL,  # contestó y colgó en <5s; sin reglas de incidencia
}

# mapeo código -> nombre para interpretar history y métricas
STATUS_TO_NAME = {
    STATUS_BUSY: "BUSY",
    STATUS_TERMINATED: "TERMINATED",
    STATUS_NOANSWER: "NOANSWER",
    STATUS_CONGESTION: "CONGESTION",
    STATUS_TIMEOUT: "TIMEOUT",
    STATUS_ANSWERED_AGENT: "ANSWERED_AGENT",
    STATUS_ANSWERED_PSTN: "ANSWERED_PSTN",
    STATUS_CHANUNAVAIL: "CHANUNAVAIL",
    STATUS_INVALID_NUMBER: "INVALID_NUMBER",
    STATUS_AMD_MACHINE: "AMD",  # AMD Detected / Contestador
    STATUS_SHORTCALL: "EXIT_SHORTCALL",
}

# contact final status
INITIAL = 0
PENDING_ATTEMPTS = 1
FINALIZED_NOCONTACT = 2
FINALIZED_SUCCESS = 3

FINAL_STATUS_TO_NAME = {
    FINALIZED_NOCONTACT: "FINALIZED WITH NO CONTACT",
    PENDING_ATTEMPTS: "NO CONTACTS WITH PENDING ATTEMPTS",
    FINALIZED_SUCCESS: "CONTACTED SUCCESSFULLY"
}

NO_DISPOSITION_OPTION = -1

# percentage called threshold for notify OML
PERCENTAGE_PENDING_CALL_THRESHOLD = 5
RECALC_LOCK_KEY = 'CAMP:DISTRIBUTION:RECALC_LOCK'
RECALC_LOCK_TTL = 2

# status types
PHONE_TYPE = 1
DISPOSITION_TYPE = 2

# fail statuses
# TODO: incorporate the names of the other fail events
FAIL_EVENTS = ['BUSY', 'NOANSWER', 'CONGESTION', 'TIMEOUT', 'TERMINATED', 'CHANUNAVAIL', 'INVALID_NUMBER', 'CANCEL', 'AMD', 'EXIT_SHORTCALL']

# fail statuses with no incidence rules
FAIL_NO_RULES_EVENTS = ['CHANUNAVAIL', 'INVALID_NUMBER', 'CANCEL', 'AMD', 'EXIT_SHORTCALL']

# incidence rules multinum behauviour
FIXED = 1
MULT = 2

JOB_STARTED = 1
JOB_FAILED = 2


class CampaignNotFoundError(Exception):
    """Raised when a campaign is expected to exist in the dialer database but does not."""
    pass


def job_handler_decorator(method):
    def wrapper(*args, **kwargs):
        worker_class = args[0]
        job = args[2]
        id_job = worker_class.insert_job(job)
        try:
            result = method(*args, **kwargs)
            worker_class.remove_job(id_job)
            return result
        except Exception as e:
            logger.exception(f"An error occurred in {method.__name__}: {e}")
            worker_class.save_job_error(id_job, str(e))
            raise e
    return wrapper


class AverageWorker(DialerWorker):
    """A worker flow with a dialing strategy, call contacts according to the available agents and
    the campaigns they are assigned to"""

    POSTGRES_OML_CONNECTION_STR = (f'postgresql://{POSTGRES_OML_USER}:{POSTGRES_OML_PASSWORD}'
                                   f'@{POSTGRES_OML_SERVER}:{POSTGRES_OML_PORT}/{POSTGRES_OML_DB}')
    POSTGRES_DIALER_CONNECTION_STR = (f'postgresql://{POSTGRES_DIALER_USER}:'
                                      f'{POSTGRES_DIALER_PASSWORD}@{POSTGRES_DIALER_SERVER}:'
                                      f'{POSTGRES_DIALER_PORT}/{POSTGRES_DIALER_DB}')
    REDIS_OML_CONNECTION = None
    REDIS_DIALER_CONNECTION = None
    POSTGRES_OML_POOL = None
    POSTGRES_DIALER_POOL = None
    GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)
    ACTIVE_CAMPAIGNS_SET = 'campaigns:active'

    @classmethod
    def _get_gearman_client(cls):
        """
        Helper para obtener el cliente Gearman.
        Retorna el cliente existente o crea uno nuevo si es necesario.
        
        Returns:
            gearman.GearmanClient: Cliente Gearman configurado
        """
        if cls.GM_CLIENT is None:
            cls.GM_CLIENT = gearman.GearmanClient(GEARMAN_JOB_SERVERS)
        return cls.GM_CLIENT

    @classmethod
    def trigger_acd_dial(cls, phone_number, campaign_id, contact_id, agent_id=None, attributes=None):
        """
        Envía un job a Gearman para ejecutar una llamada a través del ACD.
        
        Args:
            phone_number (str): Número de teléfono a llamar
            campaign_id (int): ID de la campaña
            contact_id (int): ID del contacto
            agent_id (int, optional): ID del agente (si aplica)
            attributes (dict, optional): Metadatos adicionales
            
        Returns:
            bool: True si el job se envió correctamente, False en caso de error
        """
        try:
            # Construir payload JSON
            payload = {
                "command": "dial",
                "number": phone_number,
                "campaign_id": campaign_id,
                "contact_id": contact_id,
                "agent_id": agent_id,
                "metadata": attributes or {}
            }
            
            message = json.dumps(payload)
            
            # Obtener cliente Gearman
            client = cls._get_gearman_client()
            
            # Enviar job a Gearman con background=True
            client.submit_job('acd-call-processor', message, background=True)
            
            logger.info(
                f"Job enviado a acd-call-processor para contact {contact_id} "
                f"en campaign {campaign_id}, número: {phone_number}"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Error al enviar job a acd-call-processor para contact {contact_id} "
                f"en campaign {campaign_id}: {e}",
                exc_info=True
            )
            return False

    # Housekeeping del set de idempotencia (SEEN)
    SEEN_TTL_SECONDS = int(os.getenv("SCHED_SEEN_TTL_SECONDS", 7 * 24 * 3600))  # 7 días

    # Lua: decrementa H[AGENDAS] sólo si > 0, y devuelve el nuevo valor
    _LUA_HINCR_IF_GT0 = """
    local key = KEYS[1]
    local field = ARGV[1]
    local delta = tonumber(ARGV[2])

    local cur = redis.call('HGET', key, field)
    if not cur then
        cur = 0
    else
        cur = tonumber(cur) or 0
    end

    if cur <= 0 then
        return cur
    end

    local newv = cur + delta
    if newv < 0 then newv = 0 end
    redis.call('HSET', key, field, newv)
    return newv
    """

    @classmethod
    def get_oml_connection(cls):
        if cls.POSTGRES_OML_POOL is None:
            cls.POSTGRES_OML_POOL = ConnectionPool(cls.POSTGRES_OML_CONNECTION_STR,
                                                   min_size=1, max_size=2, open=True)
        return cls.POSTGRES_OML_POOL.connection()

    @classmethod
    def get_dialer_connection(cls):
        if cls.POSTGRES_DIALER_POOL is None:
            cls.POSTGRES_DIALER_POOL = ConnectionPool(cls.POSTGRES_DIALER_CONNECTION_STR,
                                                      min_size=1, max_size=2, open=True)
        return cls.POSTGRES_DIALER_POOL.connection()

    @classmethod
    def insert_job(cls, job):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            job_unique = job.unique.decode('utf8')
            job_name = job.task.decode('utf8')
            cursor.execute(
                'INSERT INTO jobs (job_id, job_name, status) VALUES (%s, %s, %s) RETURNING id;',
                (job_unique, job_name, JOB_STARTED))
            return cursor.fetchone()[0]

    @classmethod
    def save_job_error(cls, id_job, exception):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE jobs SET status = %s, error = %s WHERE id = %s;',
                           (JOB_FAILED, exception, id_job))

    @classmethod
    def remove_job(cls, id_job):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM jobs WHERE id = %s;', (id_job,))

    @classmethod
    def system_is_active(cls):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT is_active FROM system_control;')
            return cursor_dialer.fetchone()[0]

    # --- agendas counter helpers ---
    @classmethod
    def _agendas_counter_key(cls, id_campaign):
        return f'CAMP:{id_campaign}:SCHED'

    @classmethod
    def _agendas_seen_set_key(cls, id_campaign):
        # Para evitar doble decremento por el mismo job_id
        return f'CAMP:{id_campaign}:SCHED:SEEN'

    @classmethod
    def agendas_increment(cls, id_campaign, delta=1):
        cls.connect_redis_dialer()
        key = cls._agendas_counter_key(int(id_campaign))
        try:
            cls.REDIS_DIALER_CONNECTION.hincrby(key, 'AGENDAS', int(delta))
        except Exception:
            logger.exception("agendas_increment failed camp=%s delta=%s", id_campaign, delta)

    @classmethod
    def agendas_decrement(cls, id_campaign, delta=1):
        """
        Decrementa AGENDAS de forma atómica y nunca negativa (sin idempotencia).
        Útil como fallback si algo llama agendas_decrement() directamente.
        """
        cls.connect_redis_dialer()
        key = cls._agendas_counter_key(int(id_campaign))

        try:
            dec = -abs(int(delta or 1))
            cls.REDIS_DIALER_CONNECTION.eval(
                cls._LUA_HINCR_IF_GT0,
                1,          # numkeys
                key,        # KEYS[1]
                "AGENDAS",  # ARGV[1]
                dec         # ARGV[2]
            )
        except Exception:
            logger.exception("agendas_decrement failed camp=%s delta=%s", id_campaign, delta)

    @classmethod
    def rebuild_active_campaigns_set(cls):
        cls.connect_redis_dialer()
        with cls.get_dialer_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM ONLY campaign WHERE dialer_status = %s;", (ACTIVE,))
            active_ids = [str(r[0]) for r in cur.fetchall()]
        pipe = cls.REDIS_DIALER_CONNECTION.pipeline()
        pipe.delete(cls.ACTIVE_CAMPAIGNS_SET)
        if active_ids:
            pipe.sadd(cls.ACTIVE_CAMPAIGNS_SET, *active_ids)
            for cid in active_ids:
                pipe.hset(f'CAMP:{cid}:DISTRIBUTION', 'STATUS', '1')
        pipe.execute()

    @classmethod
    def allowed_calls_prority_percentage(
        cls, id_campaign: int, contacts_attempts_number_prev: int
    ) -> int:
        cls.connect_redis_dialer()

        my_key = f'CAMP:{id_campaign}:DISTRIBUTION'
        cls.REDIS_DIALER_CONNECTION.hset(my_key, 'CALLS', int(contacts_attempts_number_prev))

        active_ids_raw = cls.REDIS_DIALER_CONNECTION.smembers(cls.ACTIVE_CAMPAIGNS_SET)
        if not active_ids_raw:
            return 0

        active_ids = []
        for cid in active_ids_raw:
            try:
                active_ids.append(int(cid))
            except (TypeError, ValueError):
                logger.warning("ID inválido en %s: %r", cls.ACTIVE_CAMPAIGNS_SET, cid)

        if not active_ids:
            return 0

        keys = [f'CAMP:{cid}:DISTRIBUTION' for cid in active_ids]
        pipe = cls.REDIS_DIALER_CONNECTION.pipeline()
        for key in keys:
            pipe.hget(key, 'STATUS')
            pipe.hget(key, 'CALLS')
        flat = pipe.execute()

        total_calls = 0
        for i in range(0, len(flat), 2):
            status = flat[i]
            calls_raw = flat[i + 1]
            if status == '1':
                try:
                    total_calls += int(calls_raw or 0)
                except (TypeError, ValueError):
                    pass

        pct_raw = cls.REDIS_DIALER_CONNECTION.hget(my_key, 'PERCENTAGE')
        try:
            percentage = float(pct_raw or 0.0)
        except (TypeError, ValueError):
            percentage = 0.0

        if total_calls <= 0:
            return 0

        assigned = int(floor(percentage * total_calls))

        if assigned < 1 and contacts_attempts_number_prev > 0:
            assigned = 1

        return min(assigned, contacts_attempts_number_prev)

    @classmethod
    def _extract_campaign_id_from_distribution_key(cls, redis_key):
        if not redis_key:
            return None
        try:
            _, campaign_id, _ = redis_key.split(':', 2)
            return int(campaign_id)
        except (ValueError, AttributeError):
            return None

    @classmethod
    def get_campaign_priority_from_db(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT priority FROM campaign WHERE id = %s;', (id_campaign,))
            result = cursor_dialer.fetchone()
            if result:
                return result[0]
        return None

    @classmethod
    def get_campaign_priority(cls, id_campaign, redis_key=None):
        cls.connect_redis_dialer()
        if redis_key is None:
            redis_key = f'CAMP:{id_campaign}:DISTRIBUTION'
        priority_value = cls.REDIS_DIALER_CONNECTION.hget(redis_key, 'PRIORITY')
        if priority_value is not None:
            return int(priority_value)
        priority_from_db = cls.get_campaign_priority_from_db(id_campaign)
        if priority_from_db is None:
            return None
        cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'PRIORITY', priority_from_db)
        return int(priority_from_db)

    @classmethod
    def _ensure_priority_cached(cls, id_campaign: int, redis_key: str) -> int | None:
        """
        Checks that the PRIORITY key is present in Redis for the campaign.
        If missing or invalid, it backfills the value from the DB and sets it.
        Returns the priority (int) or None on failure.
        """
        pr = cls.REDIS_DIALER_CONNECTION.hget(redis_key, 'PRIORITY')
        if pr is not None:
            try:
                return int(pr)
            except (TypeError, ValueError):
                pass  # invalid value, will backfill

        # Backfill from PGSQL
        pr_db = cls.get_campaign_priority_from_db(id_campaign)
        if pr_db is None or pr_db <= 0:
            logger.warning('Campaña %s sin prioridad válida en DB. Se usará 0.', id_campaign)
            # Set 0 on Redis
            cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'PRIORITY', 0)
            return 0

        cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'PRIORITY', pr_db)
        logger.info('Backfill de prioridad para campaña %s: %d', id_campaign, pr_db)
        return int(pr_db)

    @classmethod
    def update_percentages_priority_campaigns(cls):
        """
        Recalculates the PERCENTAGE for ALL active campaigns atomically and resiliently.
        It relies on the campaigns:active set and uses a distributed lock
        to prevent race conditions. The PERCENTAGE is calculated based on the PRIORITY
        of each campaign.
        """
        cls.connect_redis_dialer()

        lock_key = 'lock:update_percentages'
        if not cls.REDIS_DIALER_CONNECTION.set(lock_key, '1', nx=True, ex=30):
            logger.debug("Recálculo de porcentajes ya en progreso, saltando.")
            return

        try:
            # 1. Get all active campaign IDs
            active_ids_raw = cls.REDIS_DIALER_CONNECTION.smembers(cls.ACTIVE_CAMPAIGNS_SET)
            if not active_ids_raw:
                logger.debug('No active campaigns; skipping percentage update.')
                return

            active_ids = [int(cid) for cid in active_ids_raw if cid.isdigit()]

            # 2. Collect priorities, backfilling if necessary
            campaign_priorities = {}
            total_priority = 0
            for cid in active_ids:
                redis_key = f'CAMP:{cid}:DISTRIBUTION'
                priority = cls._ensure_priority_cached(cid, redis_key)
                # Use 0 if priority is None to participate in uniform distribution
                priority = priority or 0
                campaign_priorities[cid] = priority
                total_priority += priority

            # 3. Calculate and write new percentages in a pipeline
            pipe = cls.REDIS_DIALER_CONNECTION.pipeline()

            # Fallback to uniform distribution if no valid priorities
            if total_priority <= 0:
                logger.warning(
                    'Total priority is 0. Using uniform distribution'
                    ' for %d active campaigns.', len(active_ids)
                )
                if active_ids:
                    uniform_pct = 1.0 / len(active_ids)
                    for cid in active_ids:
                        pipe.hset(f'CAMP:{cid}:DISTRIBUTION', 'PERCENTAGE', uniform_pct)
            else:
                # Normal distribution based on priority
                for cid, priority in campaign_priorities.items():
                    percentage = float(priority) / total_priority
                    pipe.hset(f'CAMP:{cid}:DISTRIBUTION', 'PERCENTAGE', percentage)
            pipe.execute()
            logger.info("Recalculated distribution percentages for %d campaigns.", len(active_ids))

        finally:
            # 4. Release the lock
            cls.REDIS_DIALER_CONNECTION.delete(lock_key)

    @classmethod
    def process_campaign_inside(cls, id_campaign):
        while cls.campaign_is_active(id_campaign):
            logger.debug(f'\nCampaign {id_campaign} is active')
            allowed_to_call, extra_info = cls.is_allowed_to_call(id_campaign)
            if allowed_to_call:
                logger.debug(f'Campaign {id_campaign} is allowed to call')
                contacts_attempts_number_prev = cls.allowed_parallel_contact_attempts(id_campaign)
                contacts_attempts_number = cls.allowed_calls_prority_percentage(
                    id_campaign, contacts_attempts_number_prev)
                
                # Conectar Redis antes del loop para reservas atómicas
                cls.connect_redis_dialer()
                key_calls = f'OML:CALLS:{id_campaign}:DIALER'
                campaign_max = cls.get_campaign_max_available_channels(id_campaign)
                active_channels = cls.get_active_channels(id_campaign)
                
                # Log de información de canales en el ciclo
                logger.debug(
                    f'Campaign {id_campaign}: ciclo actual - '
                    f'canales_maximos={campaign_max} llamadas_actuales={active_channels} '
                    f'contactos_permitidos={contacts_attempts_number}'
                )
                
                initial_time = datetime.datetime.now()
                caps_calls_counter = 0
                contacts = cls.take_contacts(contacts_attempts_number, id_campaign)
                
                if TIME_BETWEEN_CALLS:
                    if not contacts:
                        sleep(float(TIME_BETWEEN_CALLS))
                for contact in contacts:
                    while True:
                        # we need to ensure the selected contact is eventually called
                        current_time = datetime.datetime.now()
                        current_delta = current_time - initial_time
                        if current_delta >= timedelta(seconds=1):
                            logger.debug(f"Campaign {id_campaign}: CAPS init")
                            caps_calls_counter = 0
                            initial_time = current_time
                        else:
                            if caps_calls_counter < CAPS:
                                logger.debug(
                                    f"Campaign {id_campaign}: attempt to call selected contact")
                                
                                # --- RESERVA INMEDIATA EN EL LOOP ---
                                # Incrementamos ANTES de mandar a Gearman para evitar el lag
                                current = cls.REDIS_DIALER_CONNECTION.incrby(key_calls, 1)
                                
                                # Verificación de seguridad (Double Check)
                                if current > campaign_max:
                                    # Si nos pasamos, devolvemos el cupo y abortamos este contacto
                                    cls.REDIS_DIALER_CONNECTION.decrby(key_calls, 1)
                                    cls._publish_calls_count(id_campaign)
                                    logger.warning(
                                        f"Campaign {id_campaign}: Exceeded max channels "
                                        f"({current} > {campaign_max}). Skipping contact {contact[0]}"
                                    )
                                    # Lo marcamos para reintento
                                    with cls.get_dialer_connection() as conn:
                                        conn.cursor().execute(
                                            'UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s AND id_campaign = %s',
                                            (STATUS_CREATED, contact[0], id_campaign)
                                        )
                                    break
                                
                                # Si la reserva fue exitosa, enviamos a Gearman
                                cls.attempt_contact(contact, id_campaign)
                                cls._publish_calls_count(id_campaign)
                                caps_calls_counter += 1
                                break
                            else:
                                logger.debug(f"Campaign {id_campaign}: CAPS sleep")
                                remaining = (timedelta(seconds=1) - current_delta).total_seconds()
                                sleep(remaining)
            else:
                # schedule process-campaign for the next time the campaign is allowed to run
                next_allowed_date = cls.get_next_allowed_date(id_campaign, extra_info)
                message = json.dumps({
                    'datetime_start': next_allowed_date.strftime('%d/%m/%y %H:%M:%S'),
                    'type': 'process-campaign',
                    'id_campaign': str(id_campaign),
                })
                cls.GM_CLIENT.submit_job('schedule-agenda', message)
                return None

    @classmethod
    def get_next_day_of_week_allowed(
            cls, permission_days_campaign, day_of_week, current_date, hour_start):
        # get next day of the week allowed in the campaign
        # search for an allowed day
        dow = (day_of_week + 1) % 7
        while not permission_days_campaign[dow]:
            dow = (dow + 1) % 7
        # construct the datetime
        days_until_next_day_allowed = (dow - day_of_week) % 7
        date = current_date + timedelta(days_until_next_day_allowed)
        return datetime.datetime.combine(date, hour_start)

    @classmethod
    def get_next_allowed_date(cls, id_campaign, extra_info):
        (day_of_week_allowed, day_of_week, hour_match, current_date, hour,
         minute, campaign_info) = extra_info
        (hour_start, __, monday, tuesday, wednesday, thursday, friday, saturday,
         sunday) = campaign_info
        permission_days_campaign = campaign_info[2:]
        # if the day of the week is not allowed get the next day of week allowed with hour_start
        if not day_of_week_allowed:
            return cls.get_next_day_of_week_allowed(
                permission_days_campaign, day_of_week, current_date, hour_start)
        # if current time < hour_start, just use the same day with hour_start
        # else, get the next day of week allowed with hour start
        current_time = datetime.time(hour, minute)
        if current_time < hour_start:
            return datetime.datetime.combine(current_date, hour_start)
        return cls.get_next_day_of_week_allowed(
            permission_days_campaign, day_of_week, current_date, hour_start)

    @classmethod
    def connect_redis_oml(cls):
        if cls.REDIS_OML_CONNECTION is None:
            cls.REDIS_OML_CONNECTION = redis.Redis(
                host=REDIS_OML_SERVER, port=REDIS_OML_PORT, decode_responses=True, db=0)

    @classmethod
    def connect_redis_dialer(cls):
        if cls.REDIS_DIALER_CONNECTION is None:
            cls.REDIS_DIALER_CONNECTION = redis.Redis(
                host=REDIS_DIALER_SERVER,
                port=int(REDIS_DIALER_PORT),
                decode_responses=True,
                db=REDIS_DIALER_DB,
            )

    @classmethod
    def is_allowed_to_call(cls, id_campaign):
        # check if opening hours are ok
        # TODO: a possible optimization here could be pause the campaign and place a scheduled task
        # to resume it later at the following allowed opening hour
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            return cls.opening_hours_match(cursor_dialer, id_campaign)

    @classmethod
    def get_campaign_data(cls, id_campaign, cursor_oml, contact_strategy):
        """
        Obtiene los datos de la campaña desde OML con retry automático y refresco de transacción.
        """
        logger.debug(f"Retrieving data from OML campaign with id={id_campaign}")

        # Definimos la función helper DENTRO del método (sin @classmethod)
        def execute_with_retry(query, params, context, max_retries=10, initial_delay=0.5):
            """Ejecuta una consulta con retry y REFRESCANDO la conexión."""
            for attempt in range(max_retries):
                # --- AQUÍ ESTÁ LA CLAVE DEL ARREGLO ---
                # Si fallamos la primera vez, hacemos commit en la conexión de LECTURA
                # para forzar a Postgres a refrescar el snapshot y ver datos nuevos.
                if attempt > 0:
                    try:
                        cursor_oml.connection.commit()
                    except Exception as e:
                        logger.warning(f"Error refreshing connection snapshot: {e}")
                # --------------------------------------

                cursor_oml.execute(query, params)
                row = cursor_oml.fetchone()

                if row is not None:
                    if attempt > 0:
                        logger.debug(
                            f"Campaign {id_campaign}: Successfully retrieved {context} "
                            f"on attempt {attempt + 1}"
                        )
                    return row

                if attempt < max_retries - 1:
                    # Usamos min() para que la espera no sea eterna (tope 3 seg)
                    delay = min(initial_delay * (2 ** attempt), 3.0)
                    logger.warning(
                        f"Campaign {id_campaign}: No row returned for {context} "
                        f"on attempt {attempt + 1}/{max_retries}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    sleep(delay)
                else:
                    raise ValueError(
                        f"Campaign {id_campaign}: no row returned for {context} "
                        f"after {max_retries} attempts"
                    )

        # 1. ominicontacto_app_campana
        logger.debug(f"Campaign {id_campaign}: from ominicontacto_app_campana")
        campaign_id_data = execute_with_retry(
            "SELECT id,estado,nombre,fecha_inicio,fecha_fin,control_de_duplicados,prioridad "
            "FROM ominicontacto_app_campana WHERE id = %s;",
            (id_campaign,),
            "ominicontacto_app_campana"
        )

        # 2. queue_table
        logger.debug(f"Campaign {id_campaign}: from queue_table")
        campaign_id_data += execute_with_retry(
            "SELECT strategy,wait,initial_predictive_model,initial_boost_factor,maxlen "
            "FROM queue_table WHERE campana_id = %s;",
            (id_campaign,),
            "queue_table"
        )

        # 3. ominicontacto_app_actuacionvigente
        logger.debug(f"Campaign {id_campaign}: from ominicontacto_app_actuacionvigente")
        campaign_id_data += execute_with_retry(
            "SELECT domingo,lunes,martes,miercoles,jueves,viernes,sabado,hora_desde,hora_hasta "
            "FROM ominicontacto_app_actuacionvigente WHERE campana_id = %s;",
            (id_campaign,),
            "ominicontacto_app_actuacionvigente"
        )

        logger.debug(f"Campaign {id_campaign}: setting dialer specific options")
        campaign_id_data += (contact_strategy, CREATED)

        # Las reglas de incidencia usualmente se crean en la misma transacción.
        # Si fallan, el retry debería aplicarse también, pero por ahora
        # mantenemos tu estructura original asumiendo que si la campaña apareció,
        # las reglas también.
        logger.debug(f"Campaign {id_campaign}: from incidence rules")
        cursor_oml.execute(
            "SELECT * FROM ominicontacto_app_reglasincidencia WHERE campana_id = %s;",
            (id_campaign,)
        )
        incidence_rules_data = cursor_oml.fetchall()

        logger.debug(f"Campaign {id_campaign}: from incidence rules for disposition options")
        cursor_oml.execute(
            """SELECT ric.id,ric.opcion_calificacion_id,ric.intento_max,ric.reintentar_tarde,
            ric.en_modo,opc.campana_id
            FROM ominicontacto_app_reglaincidenciaporcalificacion AS ric
            INNER JOIN ominicontacto_app_opcioncalificacion AS opc ON
            ric.opcion_calificacion_id = opc.id
            AND opc.campana_id = %s;""", (id_campaign,)
        )
        incidence_rules_disposition_data = cursor_oml.fetchall()

        # 4. Metadata (También aplicamos retry aquí por seguridad)
        metadata_row = execute_with_retry(
            """SELECT db.metadata FROM ominicontacto_app_basedatoscontacto as db
            INNER JOIN ominicontacto_app_campana AS ca ON ca.bd_contacto_id = db.id
            AND ca.id = %s;""",
            (id_campaign,),
            "basedatoscontacto.metadata join (bd_contacto_id)"
        )
        metadata = json.dumps(metadata_row[0])
        campaign_id_data += (metadata,)

        cls.connect_redis_oml()
        customdialerdst = cls.REDIS_OML_CONNECTION.hget(
            f"OML:CAMP:{id_campaign}", "CUSTOMDIALERDST"
        )
        campaign_id_data += (customdialerdst,)

        return campaign_id_data, incidence_rules_data, incidence_rules_disposition_data

    @classmethod
    @job_handler_decorator
    def edit_campaign(cls, worker, job):
        # assumes the dialer campaign exists in OML with all the required tables and fields created
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Editing the campaign {id_campaign}')
        contact_strategy = data['contact_strategy']
        # 0- pause campaign
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                orig_status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)
                cls.set_campaign_status(id_campaign, PAUSED, cursor_dialer)
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    (campaign_id_data, incidence_rules_data,
                     incidence_rules_disposition_data) = cls.get_campaign_data(
                        id_campaign, cursor_oml, contact_strategy)
                    priority = campaign_id_data[6]
                    # 1- update campaign table
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the campaign data into omnidialer')
                    cls.connect_redis_dialer()
                    customdialerdst = campaign_id_data[-1]
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:CUSTOMDIALERDST', customdialerdst)
                    voicebot = cls.REDIS_OML_CONNECTION.hget(
                        f"OML:CAMP:{id_campaign}", "VOICEBOT")
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:VOICEBOT', voicebot or 'False')
                    params = campaign_id_data[1:] + (id_campaign,)
                    cursor_dialer.execute(
                        """UPDATE campaign SET oml_status = %s, name = %s, start_date = %s,
                        end_date = %s, duplicates_control = %s, priority = %s, strategy = %s,
                        wait = %s, initial_predictive_model = %s, initial_boost_factor = %s,
                        max_channels = %s, sunday = %s, monday = %s, tuesday = %s, wednesday = %s,
                        thursday = %s, friday = %s, saturday = %s, hour_start = %s, hour_ends = %s,
                        contact_strategy = %s, dialer_status = %s, metadata = %s,
                        customdialerdst = %s
                        WHERE id = %s;""", params)
                    # 2- update incidence rules
                    cursor_dialer.execute('DELETE FROM incidence_rules WHERE campaign_id = %s',
                                          (id_campaign,))
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules into omnidialer')
                    for incidence_rule in incidence_rules_data:
                        cursor_dialer.execute("""INSERT INTO incidence_rules
                         (id, status, status_custom, max_attempt, retry_later, in_mode, campaign_id)
                         VALUES (%s, %s, %s, %s, %s, %s, %s);""", incidence_rule)
                    cursor_dialer.execute("""DELETE FROM incidence_rules_disposition
                    WHERE campaign_id = %s""", (id_campaign,))
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules for disposition'
                        ' into omnidialer')
                    for incidence_rule in incidence_rules_disposition_data:
                        cursor_dialer.execute("""INSERT INTO incidence_rules_disposition
                        (id, disposition_option_id, max_attempt, retry_later, in_mode, campaign_id)
                        VALUES (%s, %s, %s, %s, %s, %s);""", incidence_rule)
                if orig_status_campaign == ACTIVE:
                    cls.set_campaign_status(id_campaign, ACTIVE, cursor_dialer)
                    message = json.dumps({'id_campaign': id_campaign})
                    cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        try:
            cls.get_boost_factor.cache_clear()
            cls.get_campaign_max_available_channels.cache_clear()
            cls.get_incidence_rule.cache_clear()
            cls.get_incidence_rule_disposition.cache_clear()
        except AttributeError:
            pass

        cls.REDIS_DIALER_CONNECTION.hset(
            f'CAMP:{id_campaign}:DISTRIBUTION', 'PRIORITY', priority)

        cls.update_percentages_priority_campaigns()

        response = f'Campaign {id_campaign} with strategy {contact_strategy} succesfully updated!!!'

        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    def get_contacts_campaign(cls, cursor, size):
        return cursor.fetchmany(size=size)

    @classmethod
    def copy_contacts_from_oml(cls, cursor_dialer, cursor_oml, id_campaign):
        logger.debug(f'Campaign {id_campaign}: retrieving the contacts')
        sql = """SELECT co.id, co.telefono, co.datos, co.es_originario
        FROM ominicontacto_app_contacto AS co
        INNER JOIN ominicontacto_app_basedatoscontacto AS db ON
        db.id = co.bd_contacto_id INNER JOIN ominicontacto_app_campana
        AS ca ON db.id = ca.bd_contacto_id AND ca.id = %s;"""
        size = 1000
        logger.debug(f'Campaign {id_campaign}: copying the contacts')
        cursor_oml.execute(sql, (id_campaign,))
        while True:
            contacts = cls.get_contacts_campaign(cursor_oml, size)
            if not contacts:
                break
            for (id_contact, phone, data, is_original) in contacts:
                if phone != '':
                    cursor_dialer.execute(
                        'INSERT INTO contact '
                        '(id, phone, data, is_original) VALUES (%s, %s, %s, %s)'
                        'ON CONFLICT (id) DO NOTHING;',
                        (id_contact, phone, data, is_original))
                    cursor_dialer.execute(
                        """INSERT INTO contact_in_campaign (id_campaign, id_contact, status,
                    final_status, disposition_option) VALUES (%s, %s, %s, %s, %s);""",
                        (id_campaign, id_contact, STATUS_CREATED, INITIAL,
                         NO_DISPOSITION_OPTION))

    @classmethod
    @job_handler_decorator
    def create_campaign(cls, worker, job):
        # assumes the dialer campaign exists in OML with all the required tables and fields created
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Creating the campaign {id_campaign}')
        contact_strategy = data['contact_strategy']
        prefix = data.get('prefix')
        
        # Normalizar prefix: convertir lista vacía a None, o lista a string
        if prefix == []:
            prefix = None
        elif isinstance(prefix, list) and len(prefix) > 0:
            prefix = str(prefix[0])
        elif prefix is not None:
            prefix = str(prefix)
        # Si prefix es None, se mantiene como None (aceptado por PostgreSQL)
        
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    (campaign_id_data, incidence_rules_data,
                    incidence_rules_disposition_data) = cls.get_campaign_data(
                        id_campaign, cursor_oml, contact_strategy)
                    cls.connect_redis_dialer()
                    customdialerdst = campaign_id_data[-1]
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:CUSTOMDIALERDST', customdialerdst)
                    voicebot = cls.REDIS_OML_CONNECTION.hget(
                        f"OML:CAMP:{id_campaign}", "VOICEBOT")
                    cls.REDIS_DIALER_CONNECTION.set(
                        f'CAMP:{id_campaign}:VOICEBOT', voicebot or 'False')
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the campaign data into omnidialer')
                    campaign_id_data = campaign_id_data + (prefix,)
                    priority = campaign_id_data[6]
                    cursor_dialer.execute(
                        """INSERT INTO campaign (id, oml_status, name, start_date, end_date,
                        duplicates_control, priority, strategy, wait, initial_predictive_model,
                        initial_boost_factor, max_channels, sunday, monday, tuesday, wednesday,
                        thursday, friday, saturday, hour_start, hour_ends, contact_strategy,
                        dialer_status, metadata, customdialerdst, prefix) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s);""", campaign_id_data)
                    logger.debug(
                        f'Campaign {id_campaign}: inserting the incidence_rules into omnidialer')
                    for incidence_rule in incidence_rules_data:
                        cursor_dialer.execute(
                            """INSERT INTO incidence_rules
                            (id, status, status_custom, max_attempt, retry_later, in_mode,
                            campaign_id) VALUES (%s, %s, %s, %s, %s, %s, %s);""", incidence_rule)
                    logger.debug(f'Campaign {id_campaign}: Inserting the incidence_rules for'
                                ' disposition option into omnidialer')
                    for incidence_rule in incidence_rules_disposition_data:
                        cursor_dialer.execute(
                            """INSERT INTO incidence_rules_disposition
                            (id, disposition_option_id, max_attempt, retry_later, in_mode,
                            campaign_id) VALUES (%s, %s, %s, %s, %s, %s);""", incidence_rule)
                    cls.copy_contacts_from_oml(cursor_dialer, cursor_oml, id_campaign)
                    cls.REDIS_DIALER_CONNECTION.set(f'OML:CALLS:{id_campaign}:DIALER', 0)
                    cls.REDIS_DIALER_CONNECTION.hset(
                        f'CAMP:{id_campaign}:DISTRIBUTION', 'PRIORITY', priority)
                    cls.REDIS_OML_CONNECTION.publish(
                        'OML:CHANNEL:DIALER',
                        json.dumps({'type': 'CREATE',
                                    'camp_id': id_campaign}))
                    cls.REDIS_OML_CONNECTION.publish(
                        'OML:CHANNEL:DIALER', json.dumps({'type': 'CALLS',
                                                        'camp_id': id_campaign,
                                                        'calls': 0}))

        cls.update_percentages_priority_campaigns()

        response = f'Campaign {id_campaign} with strategy {contact_strategy} created!!!'

        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    def clean_selected_contacts(cls, id_campaign):
        # set contacts marked as SELECTED_CALL back to
        # CREATED status, so they can be consumed by the process campaign
        # this is due to these contacts were marked and not called
        # or at least we didn't receive events from Asterisk to change their state
        logger.debug(f'Campaign {id_campaign}: cleaning broken selected contacts')
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE contact_in_campaign SET status = %s WHERE'
                ' id_campaign = %s and status = %s;',
                (STATUS_CREATED, id_campaign, STATUS_SELECTED_CALL))
            row_count = cursor.rowcount
            if row_count > 0:
                logger.debug(
                    f"Campaign {id_campaign}: cleaned broken selected contacts={row_count}")

    @classmethod
    def check_running_job(cls, id_campaign):
        first_running_job = cls.REDIS_DIALER_CONNECTION.set(
            f'PROCESS-CAMPAIGN-{id_campaign}', 'True', nx=True)
        if not first_running_job:
            logger.debug(f'Campaign {id_campaign}: is already running')
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'ALREADY_RUNNING',
                            'camp_id': id_campaign}))
        return first_running_job

    @classmethod
    @job_handler_decorator
    def start_campaign(cls, worker, job):
        cls.connect_redis_oml()
        if not cls.system_is_active():
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'SYSTEM_STOPPED',
                            'camp_id': 'all'}))
            return b'Forbidden operation'
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        cls.clean_selected_contacts(id_campaign)
        logger.debug(f'Campaign {id_campaign}: starting the campaign')
        cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=sync_omnileads)
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        return b'Campaign started!'

    @classmethod
    def opening_hours_match(cls, cursor, id_campaign):
        cursor.execute('SELECT EXTRACT(DOW FROM CURRENT_DATE) AS day_of_week;')
        day_of_week_int = int(cursor.fetchone()[0])
        day_of_week = WEEK_DAYS[day_of_week_int]
        cursor.execute(f'SELECT {day_of_week} FROM ONLY campaign WHERE id = %s;', (id_campaign,))
        day_of_week_allowed = cursor.fetchone()[0]
        cursor.execute('SELECT * FROM ONLY campaign WHERE id = %s AND CURRENT_TIME BETWEEN'
                       ' hour_start AND hour_ends;', (id_campaign,))
        hour_match = cursor.fetchone()
        if not day_of_week_allowed:
            logger.debug(f'Campaign {id_campaign}: day week not allowed to call')
        elif not hour_match:
            logger.debug(f'Campaign {id_campaign}: in the current time is not allowed to call')
        result = day_of_week_allowed and hour_match
        extra_info = None
        if not result:
            cursor.execute('SELECT EXTRACT(HOUR FROM NOW()) AS current_hour, '
                           'EXTRACT(MINUTE FROM NOW()) AS current_minute;')
            hour, minute = cursor.fetchone()
            hour = int(hour)
            minute = int(minute)
            cursor.execute('SELECT hour_start,hour_ends,'
                           'monday,tuesday,wednesday,thursday,friday,saturday,sunday'
                           ' FROM campaign where id = %s;', (id_campaign,))
            campaign_info = cursor.fetchone()
            cursor.execute('SELECT CURRENT_DATE;')
            current_date = cursor.fetchone()[0]
            extra_info = (day_of_week_allowed, day_of_week_int, hour_match, current_date, hour,
                          minute, campaign_info)
        return result, extra_info

    @classmethod
    def get_campaign_status(cls, id_campaign, dialer_cursor):
        dialer_cursor.execute(
            'SELECT dialer_status FROM ONLY campaign WHERE id = %s', (id_campaign,))
        row = dialer_cursor.fetchone()
        if row is None:
            raise CampaignNotFoundError(
                f'Campaign {id_campaign} does not exist in the dialer database')
        return row[0]

    @classmethod
    def all_contacts_were_attempted(cls, id_campaign):
        cls.connect_redis_dialer()
        pending_initial_contact_attempts = cls.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{id_campaign}:COUNTER',
            'PENDING_INITIAL_CONTACT_ATTEMPTS') or -1
        return int(pending_initial_contact_attempts) == 0

    @classmethod
    def no_active_incidence_rules(cls, id_campaign):
        cls.connect_redis_dialer()
        pending_attempts = cls.REDIS_DIALER_CONNECTION.hget(
            f'CAMP:{id_campaign}:COUNTER',
            FINAL_STATUS_TO_NAME[PENDING_ATTEMPTS]
        )
        return int(pending_attempts or 0) == 0

    @classmethod
    def no_active_agendas(cls, id_campaign):
        """
        True  => NO hay agendas activas para la campaña
        False => Sí hay agendas (contador > 0 o se detectan jobs en el scheduler)
        """
        str_id_campaign = str(id_campaign)

        # 0) Chequear contador primero (fast-path, resistente a caídas del jobstore)
        try:
            cls.connect_redis_dialer()
            key = cls._agendas_counter_key(id_campaign)
            cur = cls.REDIS_DIALER_CONNECTION.hget(key, 'AGENDAS')
            if int(cur or 0) > 0:
                return False  # hay agendas, no finalizar
        except Exception as exc:
            logger.warning('Agendas counter unavailable (%s). Falling back to scheduler.', exc)

        # 1) Función auxiliar para matchear jobs por nombre/args/kwargs
        def _job_matches_campaign(job):
            name = getattr(job, 'name', '') or ''
            if str_id_campaign in name:
                return True
            args = getattr(job, 'args', ()) or ()
            kwargs = getattr(job, 'kwargs', {}) or {}
            if any(str(a) == str_id_campaign for a in args):
                return True
            if any(str(v) == str_id_campaign for v in kwargs.values()):
                return True
            return False

        # 2) Reintentos breves por error transitorio del jobstore (Redis)
        attempts = 3
        for i in range(attempts):
            try:
                jobs = SchedulerWorker.SCHEDULER.get_jobs()
                break
            except Exception as exc:
                logger.warning('Scheduler get_jobs() failed (try %d/%d): %s', i + 1, attempts, exc)
                time.sleep(0.1 * (i + 1))
        else:
            # Falló todo: FAIL-SAFE => asumimos que HAY agendas (para no finalizar por glitch)
            logger.error('Scheduler get_jobs() unavailable; fail-safe: assuming agendas exist')
            return False

        # 3) Si encontramos un job matcheando, hay agendas
        for job in jobs:
            if _job_matches_campaign(job):
                return False

        # 4) Ni contador ni scheduler indican agendas: no hay
        return True

    @classmethod
    def campaign_is_active(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status = cls.get_campaign_status(id_campaign, cursor_dialer)
            # notify to OML if the campaign is outdated and pause the campaign
            cursor_dialer.execute(
                """SELECT id
                FROM ONLY campaign
                WHERE end_date >= now()::date
                AND start_date <= now()::date
                AND id = %s;""", (id_campaign,))
            campaign_in_range = cursor_dialer.fetchone()
            if not campaign_in_range:
                logger.debug(f'Campaign {id_campaign}: campaign expired')
                cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=True)
                cls.connect_redis_oml()
                cls.REDIS_OML_CONNECTION.publish(
                    'OML:CHANNEL:DIALER',
                    json.dumps({'type': 'EXPIRATION',
                                'camp_id': id_campaign}))
                return False

            # notify to OML if there are no more contacts for call and pause
            # the campaign
            if cls.all_contacts_were_attempted(id_campaign) and \
               cls.no_active_incidence_rules(id_campaign) and \
               cls.no_active_agendas(id_campaign):
                logger.debug(f'Campaign {id_campaign}: no more contacts pending for call')
                cls.set_campaign_status(id_campaign, FINALIZED, sync_omnileads=True)
                cls.connect_redis_oml()
                cls.REDIS_OML_CONNECTION.publish(
                    'OML:CHANNEL:DIALER',
                    json.dumps({
                        'type': 'CONTACTS_CONSUMED',
                        'camp_id': id_campaign,
                    }))
                return False

            # notify to OML if there are less than
            # PERCENTAGE_PENDING_CALL_THRESHOLD%
            # of contacts pending for call
            # TODO: not sure about the frequency of this notification
            cursor_dialer.execute(
                """SELECT status, count(*) * 100.0 / (
                    SELECT count(*) FROM contact_in_campaign
                    WHERE id_campaign = %s
                )
                FROM ONLY contact_in_campaign
                WHERE status = %s AND id_campaign = %s
                GROUP BY status;""",
                (id_campaign, STATUS_ANSWERED_AGENT, id_campaign)
            )
            percentage_called = cursor_dialer.fetchone()
            percentage_called = percentage_called[1] if percentage_called is not None else 0
            percentage_pending_call = 100 - percentage_called
            if percentage_pending_call <= PERCENTAGE_PENDING_CALL_THRESHOLD:
                logger.debug(f'Campaign {id_campaign}: less than '
                             f'{PERCENTAGE_PENDING_CALL_THRESHOLD}% contacts pending for call')
                cls.connect_redis_oml()
                cls.REDIS_OML_CONNECTION.publish(
                    'OML:CHANNEL:DIALER',
                    json.dumps({
                        'type': 'ALMOST_NO_CONTACTS',
                        'camp_id': id_campaign,
                    }))
        return status == ACTIVE

    @classmethod
    def get_number_active_campaigns(cls):
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT Count(*) FROM ONLY campaign WHERE dialer_status = %s""",
                (ACTIVE,))
            active_campaigns = cursor.fetchone()[0]
        return active_campaigns

    @classmethod
    def get_agent_ids_campaign(cls, id_campaign):
        with cls.get_oml_connection() as conn_oml:
            cursor_oml = conn_oml.cursor()
            TYPE_DIALER = 2
            STATUS_ACTIVE = 2
            cursor_oml.execute(
                """SELECT ominicontacto_app_agenteprofile.id, COUNT(queue_table.campana_id)
                AS queue__campana__count
                FROM ominicontacto_app_agenteprofile LEFT OUTER JOIN queue_member_table ON
                (ominicontacto_app_agenteprofile.id = queue_member_table.member_id)
                LEFT OUTER JOIN queue_table ON (queue_member_table.queue_name = queue_table.name)
                LEFT OUTER JOIN ominicontacto_app_campana ON
                (queue_table.campana_id = ominicontacto_app_campana.id)
                WHERE ominicontacto_app_agenteprofile.id in
                (
                SELECT ominicontacto_app_agenteprofile.id FROM ominicontacto_app_agenteprofile
                INNER JOIN queue_member_table ON
                (ominicontacto_app_agenteprofile.id = queue_member_table.member_id)
                INNER JOIN queue_table ON (queue_member_table.queue_name = queue_table.name)
                WHERE queue_table.campana_id = %s
                ) AND ominicontacto_app_campana.type = %s AND ominicontacto_app_campana.estado = %s
                GROUP BY ominicontacto_app_agenteprofile.id;""",
                (id_campaign, TYPE_DIALER, STATUS_ACTIVE))
            return dict(cursor_oml.fetchall())

    @classmethod
    def get_number_available_agents(cls, id_campaign):
        cls.connect_redis_oml()
        agents_distribution = cls.get_agent_ids_campaign(id_campaign)
        agents_available = 0
        total_agents_available = 0
        for key in cls.REDIS_OML_CONNECTION.scan_iter(match='OML:AGENT:*', count=1000):
            id_agent = int(key.split(':')[-1])
            if agents_distribution.get(id_agent):
                status = cls.REDIS_OML_CONNECTION.hget(key, 'STATUS')
                if status == 'READY':
                    agents_available += (1 / agents_distribution[id_agent])
                    total_agents_available += 1
        if agents_available < 1:
            if agents_available > 0:
                # there is at least one agent active
                return 1, total_agents_available
            return 0, total_agents_available
        return int(agents_available), total_agents_available

    @classmethod
    def get_active_channels(cls, id_campaign: int) -> int:
        cls.connect_redis_dialer()
        key = f'OML:CALLS:{id_campaign}:DIALER'

        val = cls.REDIS_DIALER_CONNECTION.get(key)
        if val is None:
            created = cls.REDIS_DIALER_CONNECTION.set(key, 0, nx=True)
            logger.debug(
                'OML:CALLS:%s:DIALER missing; init=%s',
                id_campaign, bool(created)
            )
            if created:
                try:
                    cls.connect_redis_oml()
                    cls.REDIS_OML_CONNECTION.publish(
                        'OML:CHANNEL:DIALER',
                        json.dumps(
                            {'type': 'CALLS', 'camp_id':
                             id_campaign, 'calls': 0}
                        )
                    )
                except Exception as e:
                    logger.debug('Publish init CALLS failed: %s', e)
            return 0

        try:
            n = int(val)
        except (TypeError, ValueError):
            logger.warning(
                'Invalid CALLS "%s" camp %s; resetting to 0', val, id_campaign
            )
            cls.REDIS_DIALER_CONNECTION.set(key, 0)
            return 0
        if n < 0:
            logger.warning(
                'Negative CALLS %s for camp %s; fixing to 0', n, id_campaign
            )
            cls.REDIS_DIALER_CONNECTION.set(key, 0)
            return 0

        return n

    @classmethod
    def _publish_calls_count(cls, id_campaign: int) -> None:
        """
        Publica el valor actual de OML:CALLS:{id_campaign}:DIALER en OML:CHANNEL:DIALER
        para que la supervisión actualice "Canales discando" en tiempo real.
        """
        try:
            cls.connect_redis_dialer()
            key = f'OML:CALLS:{id_campaign}:DIALER'
            val = cls.REDIS_DIALER_CONNECTION.get(key)
            if val is None:
                count = 0
            else:
                try:
                    count = int(val)
                except (TypeError, ValueError):
                    count = 0
            cls.connect_redis_oml()
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'CALLS', 'camp_id': id_campaign, 'calls': count})
            )
        except Exception as e:
            logger.debug('Publish CALLS count failed camp %s: %s', id_campaign, e)

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_campaign_max_available_channels(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT max_channels FROM ONLY campaign WHERE id = %s;',
                                  (id_campaign,))
            return cursor_dialer.fetchone()[0]

    @classmethod
    def get_allowed_attempts_according_agents(
        cls, id_campaign, active_channels, campaign_max_available_channels
    ):
        """
        Devuelve cuántos intentos NUEVOS puede iniciar la campaña según:
        - canales ya activos de la campaña
        - canales máximos configurados para la campaña
        - agentes disponibles en la campaña
        (la "justicia" entre campañas ya la maneja allowed_calls_prority_percentage)
        """
        available_agents, total_available_agents = cls.get_number_available_agents(id_campaign)

        logger.debug(
            "Campaign %s: available_agents=%s total_available_agents=%s",
            id_campaign, available_agents, total_available_agents
        )

        # No hay más canales libres configurados para esta campaña
        if active_channels >= campaign_max_available_channels:
            logger.debug(
                "Campaign %s: no free channels (active=%s, max=%s)",
                id_campaign, active_channels, campaign_max_available_channels
            )
            return 0

        # Si no hay agentes para ESTA campaña, no disques
        if available_agents <= 0:
            logger.debug(
                "Campaign %s: no available agents for this campaign (avail=%s)",
                id_campaign, available_agents
            )
            return 0

        # Máximo de canales que me gustaría tener para esta campaña según agentes
        # (1 canal por agente "equivalente")
        desired_total_channels = min(available_agents, campaign_max_available_channels)

        headroom = desired_total_channels - active_channels
        logger.debug(
            "Campaign %s: desired_total_channels=%s headroom=%s",
            id_campaign, desired_total_channels, headroom
        )

        if headroom <= 0:
            logger.debug(
                "Campaign %s: already at desired load (headroom<=0)",
                id_campaign
            )
            return 0

        allowed = int(headroom)
        logger.debug(
            "Campaign %s: allowed_attempts_according_agents=%s",
            id_campaign, allowed
        )
        return allowed

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_boost_factor(cls, id_campaign):
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute('SELECT initial_boost_factor FROM ONLY campaign WHERE id = %s',
                                  (id_campaign,))
            boost_factor = cursor_dialer.fetchone()[0]
            return boost_factor

    @classmethod
    def allowed_parallel_contact_attempts(cls, id_campaign):
        """
        Calculates how many NEW calls this campaign can originate in the current cycle.

        - In "predictive" mode (CUSTOMDIALERDST == '0' and VOICEBOT not True):
            Desired Target = available_agents_score * boost_factor
            New calls = Target - active_channels, limited by:
                * Campaign's max_channels
                * Actual available channels (num_available_channels)

        - In "power dialer" mode (CUSTOMDIALERDST != '0' or CAMP:{id}:VOICEBOT=True):
            Simply fills up to max_channels, respecting the available free channels.
        """
        # 1) campaign status
        active_channels = cls.get_active_channels(id_campaign)
        campaign_max_available_channels = cls.get_campaign_max_available_channels(id_campaign)

        # Free channels according to campaign config (campaign hard cap)
        num_available_channels = campaign_max_available_channels - active_channels
        num_available_channels = max(num_available_channels, 0)

        logger.debug(
            "Campaign %s: active_channels=%s campaign_max_available_channels=%s "
            "num_available_channels=%s",
            id_campaign, active_channels, campaign_max_available_channels, num_available_channels
        )

        # 2) POWER DIALER mode (customdest different from '0' or VOICEBOT=True):
        #    here the idea is simply to fill channels up to the maximum.
        customdialerdst = cls.REDIS_DIALER_CONNECTION.get(f'CAMP:{id_campaign}:CUSTOMDIALERDST')
        voicebot = cls.REDIS_DIALER_CONNECTION.get(f'CAMP:{id_campaign}:VOICEBOT')
        is_power_dialer = (
            (customdialerdst is not None and customdialerdst != '0')
            or (voicebot and str(voicebot).lower() == 'true')
        )
        if is_power_dialer:
            reason = 'VOICEBOT=True' if (voicebot and str(voicebot).lower() == 'true') else f'CUSTOMDIALERDST={customdialerdst!r}'
            logger.debug(
                "Campaign %s: %s => POWER DIALER mode, "
                "allowed_parallel_contact_attempts=%s",
                id_campaign, reason, num_available_channels
            )
            return num_available_channels

        # 3) PREDICTIVE mode: use agents + boost_factor
        #    available_agents_score is already weighted by number of campaigns/queues.
        available_agents_score, total_agents_available = (
            cls.get_number_available_agents(id_campaign)
        )
        logger.debug(
            "Campaign %s: available_agents_score=%s total_agents_available=%s",
            id_campaign, available_agents_score, total_agents_available
        )

        # If there are no available agents for THIS campaign, mark none.
        if available_agents_score <= 0:
            logger.debug("Campaign %s: no available agents for this campaign, returning 0",
                         id_campaign)
            return 0

        # boost_factor puede venir como Decimal/None/float; normalizamos a float
        raw_boost = cls.get_boost_factor(id_campaign)
        try:
            boost_factor = float(raw_boost or 1.0)
        except (TypeError, ValueError):
            boost_factor = 1.0

        # 4) Capacidad deseada (target): cuántas llamadas QUEREMOS tener activas
        #    Fórmula: agentes_equivalentes * boost_factor
        target_concurrent_calls = available_agents_score * boost_factor

        # Redondeo agresivo hacia arriba para no perder fracciones
        # Ej: 0.5 * 1.5 = 0.75 => ceil(0.75) = 1
        target_capped = min(ceil(target_concurrent_calls), campaign_max_available_channels)

        # 5) Delta: cuántas llamadas faltan para llegar al target
        calls_to_dial = target_capped - active_channels

        logger.debug(
            "Campaign %s: boost_factor=%s target_concurrent_calls=%s "
            "target_capped=%s calls_to_dial(before caps)=%s",
            id_campaign, boost_factor, target_concurrent_calls,
            target_capped, calls_to_dial
        )

        # Si estamos en o por encima del target, pero hay agentes y canales disponibles,
        # permitir al menos 1 llamada para mantener el target activo
        # (esto evita que el dialer se detenga cuando target == active_channels)
        if calls_to_dial <= 0:
            if available_agents_score > 0 and num_available_channels > 0:
                # Hay capacidad disponible: permitir 1 llamada para mantener el target
                calls_to_dial = 1
                logger.debug(
                    "Campaign %s: at target but allowing 1 call to maintain active load "
                    "(agents=%s, channels_available=%s)",
                    id_campaign, available_agents_score, num_available_channels
                )
            else:
                logger.debug(
                    "Campaign %s: already at or above desired load (calls_to_dial<=0). Returning 0.",
                    id_campaign
                )
                return 0

        # 6) Respetar canales libres reales
        final_allowed = min(calls_to_dial, num_available_channels)
        final_allowed = max(int(final_allowed), 0)

        logger.debug(
            "Campaign %s: final_allowed_parallel_contact_attempts=%s "
            "(after channel cap num_available_channels=%s)",
            id_campaign, final_allowed, num_available_channels
        )

        return final_allowed

    @classmethod
    def take_contacts(cls, contacts_attempts_number, id_campaign):
        logger.debug("Campaign {0}: contacts_attempts_number={1}".format(
            id_campaign, contacts_attempts_number))
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute("""UPDATE contact_in_campaign as cc
                                     SET status = %s,
                                     schedule_aborted = false
                                     FROM contact as co
                                     WHERE cc.id IN (SELECT id
                                     FROM ONLY contact_in_campaign
                                     WHERE id_campaign = %s and
                                     (status = %s OR schedule_aborted = true)
                                     LIMIT %s) AND co.id = cc.id_contact
                                     RETURNING cc.id_contact, cc.id_campaign, co.phone;""",
                                  (STATUS_SELECTED_CALL, id_campaign, STATUS_CREATED,
                                   contacts_attempts_number))
            contacts = cursor_dialer.fetchall()
            logger.debug("Campaign {0}: selected {1} contacts".format(id_campaign, len(contacts)))
            return contacts

    @classmethod
    def attempt_contact(cls, contact, id_campaign):
        message = json.dumps({'contact': contact, 'id_campaign': id_campaign})
        # TODO: think if the following could be a background job call
        cls.GM_CLIENT.submit_job('process-contact', message)





    @classmethod
    def is_blacklisted(cls, phone_number):
        """
        Verifica si un número está en la lista negra de OML.
        Retorna True si está bloqueado, False si puede ser llamado.
        """
        if not phone_number:
            return False

        cls.connect_redis_oml()
        black_list_key = 'OML:BLACKLIST'

        try:
            # sismember retorna 1 si existe, 0 si no. En Python bool(1) es True.
            is_black_listed = cls.REDIS_OML_CONNECTION.sismember(
                black_list_key, phone_number
            )
            if is_black_listed:
                logger.warning(
                    f"BLOCKED: Phone number {phone_number} is in BLACKLIST"
                )
            return bool(is_black_listed)
        except Exception as e:
            logger.error(f"Error checking BLACKLIST for {phone_number}: {e}")
            # Ante error de Redis, decidimos si bloquear o permitir.
            # Por seguridad (fail-open vs fail-close), aquí permitimos llamar
            # (False), pero podrías retornar True si prefieres bloquear
            # ante la duda.
            return False

    @classmethod
    @job_handler_decorator
    def process_contact(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        contact = data['contact']
        id_contact = contact[0]
        phone_number = contact[2]

        # --- CHECK BLACKLIST ---
        if cls.is_blacklisted(phone_number):
            logger.info(
                f"Campaign {id_campaign}: Contact {id_contact} "
                f"({phone_number}) skipped (BLACKLIST)"
            )

            # Finalizamos el contacto en la DB para que no se vuelva a intentar
            with cls.get_dialer_connection() as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET final_status = %s, '
                    'status = %s, schedule_aborted = false '
                    'WHERE id_contact = %s AND id_campaign = %s',
                    (FINALIZED_NOCONTACT, STATUS_CHANUNAVAIL, id_contact,
                     id_campaign)
                    # Usamos CHANUNAVAIL o un estatus específico si tuvieras
                    # STATUS_BLACKLISTED
                )

            # Liberamos la reserva que se hizo en el loop
            cls.connect_redis_dialer()
            key_calls = f'OML:CALLS:{id_campaign}:DIALER'
            cls.REDIS_DIALER_CONNECTION.decrby(key_calls, 1)
            cls._publish_calls_count(id_campaign)
            
            # Actualizamos Redis para estadísticas
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{id_contact}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT
            )
            return b'Contact skipped: Blacklisted'

        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            status_campaign = cls.get_campaign_status(id_campaign, cursor_dialer)
            
            if status_campaign == ACTIVE:
                if cls.is_allowed_to_call(id_campaign)[0]:
                    
                    # --- NOTA: La reserva ya se hizo en process_campaign_inside ---
                    # Solo necesitamos hacer rollback si falla
                    cls.connect_redis_dialer()
                    key_calls = f'OML:CALLS:{id_campaign}:DIALER'
                    
                    try:
                        logger.debug(f'Calling contact {id_contact} (reservation already made)')
                        # Aplicar prefijo si existe; publicar en cola Gearman (ACD hace ruta/troncal)
                        phone_to_dial = phone_number
                        prefix = cls.get_prefix(id_campaign)
                        if prefix:
                            try:
                                phone_to_dial = prefix[0] + phone_number
                            except Exception:
                                phone_to_dial = str(prefix) + phone_number
                        success = cls.trigger_acd_dial(
                            phone_to_dial, id_campaign, id_contact, attributes=None
                        )
                        if not success:
                            raise Exception("Failed to send dial job to Gearman")
                        
                        # Contabilizar el intento exitoso
                        cls.REDIS_DIALER_CONNECTION.hincrby(
                            f'CAMP:{id_campaign}:COUNTER', 'ATTEMPTED_CALLS'
                        )
                        return b'Contact was called'
                        
                    except Exception as e:
                        # --- ROLLBACK DE RESERVA ---
                        # Si falló el envío a Gearman, liberamos la reserva
                        try:
                            calls = cls.REDIS_DIALER_CONNECTION.decrby(key_calls, 1)
                            # Protección contra valores negativos
                            if calls < 0:
                                cls.REDIS_DIALER_CONNECTION.set(key_calls, 0)
                                logger.warning(
                                    f"Redis counter went negative for campaign {id_campaign}, "
                                    f"reset to 0"
                                )
                            cls._publish_calls_count(id_campaign)
                        except Exception as redis_error:
                            logger.error(
                                f"Error during rollback of Redis counter: {redis_error}"
                            )
                        logger.error(f"Error sending dial job to Gearman, reservation rolled back: {e}")
                        
                        # Marcamos para reintento en DB
                        cursor_dialer.execute(
                            'UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s AND id_campaign = %s',
                            (STATUS_CREATED, id_contact, id_campaign)
                        )
                        return b'Contact call failed, marked for retry'
                else:
                    # No está permitido llamar (horario), liberamos la reserva
                    cls.connect_redis_dialer()
                    key_calls = f'OML:CALLS:{id_campaign}:DIALER'
                    cls.REDIS_DIALER_CONNECTION.decrby(key_calls, 1)
                    cls._publish_calls_count(id_campaign)
                    
                    # Marcamos para reintento
                    cursor_dialer.execute(
                        'UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s AND id_campaign = %s',
                        (STATUS_CREATED, id_contact, id_campaign)
                    )
                    return b'Contact skipped: Not allowed to call (hours)'
            else:
                # Campaña no activa, liberamos la reserva
                cls.connect_redis_dialer()
                key_calls = f'OML:CALLS:{id_campaign}:DIALER'
                cls.REDIS_DIALER_CONNECTION.decrby(key_calls, 1)
                cls._publish_calls_count(id_campaign)
                
                # Marcamos para reintento
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status = %s WHERE id_contact = %s AND id_campaign = %s',
                    (STATUS_CREATED, id_contact, id_campaign)
                )
                return b'Contact skipped: Campaign not active'

    @classmethod
    @job_handler_decorator
    def pause_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        logger.debug(f'Campaign {id_campaign} pausing the campaign')
        cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=sync_omnileads)
        response = f'Campaign {id_campaign} was paused!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @job_handler_decorator
    def resume_campaign(cls, worker, job):
        cls.connect_redis_oml()
        if not cls.system_is_active():
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'SYSTEM_STOPPED',
                            'camp_id': 'all'}))
            return b'Forbidden operation'
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        cls.clean_selected_contacts(id_campaign)
        logger.debug(f'Campaign {id_campaign} resuming the campaign')
        cls.set_campaign_status(id_campaign, ACTIVE, sync_omnileads=sync_omnileads)
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        response = f'Campaign {id_campaign} was resumed!'
        response = json.dumps({'msg': response})
        return bytes(response, encoding='UTF8')

    @classmethod
    @job_handler_decorator
    def process_campaign(cls, worker, job):
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        id_campaign = cls.decode_payload(job.data)['id_campaign']
        first_running_job = cls.check_running_job(id_campaign)
        if not first_running_job:
            return b'Campaign already running'
        logger.debug(f'Campaign {id_campaign}: resuming the campaign')
        try:
            cls.process_campaign_inside(id_campaign)
            response = f'Campaign {id_campaign} process ended!'
            response = json.dumps({'msg': response})
            return bytes(response, encoding='UTF8')
        except Exception as e:
            raise e
        finally:
            cls.REDIS_DIALER_CONNECTION.delete(f'PROCESS-CAMPAIGN-{id_campaign}')

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_prefix(cls, id_campaign):
        # Campaña 0: no hay prefijo configurado en BD
        if id_campaign == 0:
            return None
        logger.debug(f'Campaign {id_campaign}: getting prefix')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT prefix FROM campaign WHERE'
                ' id = %s;',
                (id_campaign,))
            return cursor_dialer.fetchone()

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_incidence_rule(cls, id_campaign, status):
        logger.debug(f'Campaign {id_campaign}: getting the incidence rule for {status}')
        status_code = NAME_TO_STATUS[status]
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT retry_later, max_attempt, in_mode FROM ONLY incidence_rules WHERE'
                ' campaign_id = %s AND status = %s;',
                (id_campaign, status_code))
            return cursor_dialer.fetchone()

    @classmethod
    @timed_lru_cache(seconds=600, maxsize=128)
    def get_incidence_rule_disposition(cls, id_campaign, disposition_option):
        logger.debug(f'Campaign {id_campaign}: getting the incidence rule for '
                     f'disposition {disposition_option}')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'SELECT retry_later, max_attempt, in_mode FROM ONLY incidence_rules_disposition'
                ' WHERE campaign_id = %s AND disposition_option_id = %s;',
                (id_campaign, disposition_option))
            return cursor_dialer.fetchone()

    @classmethod
    def get_next_phone_number(cls, cursor_dialer, id_campaign, contact_id, phone_number):
        phone_number_index = cls.REDIS_DIALER_CONNECTION.hget(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX')
        if phone_number_index is None:
            # first time: the data is only encoded in Postgres (tables campaign & contact)
            # proceding to decode it
            cursor_dialer.execute('SELECT metadata FROM ONLY campaign WHERE id = %s',
                                  (id_campaign,))
            metadata = json.loads(cursor_dialer.fetchone()[0])
            phone_number_indexes = metadata['cols_telefono'][1:]
            cursor_dialer.execute(
                """SELECT co.data FROM ONLY contact_in_campaign AS cc
                INNER JOIN contact AS co on cc.id_contact = co.id
                WHERE cc.id_campaign = %s AND cc.id_contact = %s;""", (id_campaign, contact_id))
            data = json.loads(cursor_dialer.fetchone()[0])
            phone_number_index = 0
            phone_numbers = [phone_number]
            for i in phone_number_indexes:
                phone_numbers.append(data[i - 1])
            cursor_dialer.execute('UPDATE contact_in_campaign SET phone_numbers_list = %s WHERE'
                                  ' id_campaign = %s AND id_contact = %s;',
                                  (phone_numbers, id_campaign, contact_id))
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST',
                json.dumps(phone_numbers))
        else:
            phone_numbers = json.loads(cls.REDIS_DIALER_CONNECTION.hget(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_LIST'))
        phone_number_index = (int(phone_number_index) + 1) % len(phone_numbers)
        # update Postgres & Redis
        cursor_dialer.execute('UPDATE contact_in_campaign SET phone_number_index = %s'
                              ' WHERE id_campaign = %s AND id_contact = %s;',
                              (phone_number_index, id_campaign, contact_id))
        cls.REDIS_DIALER_CONNECTION.hset(
            f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'PHONE_NUMBER_INDEX', phone_number_index)
        return phone_numbers[phone_number_index]

    @classmethod
    def get_delay(cls, retry_later, type_incidence_rule, attempt_number):
        if type_incidence_rule == FIXED:
            return retry_later
        # MULT
        return retry_later * attempt_number

    @classmethod
    def apply_incidence_rule(
            cls, cursor_dialer, incidence_rule, contact_id, id_campaign, status, status_type,
            phone_number):
        if incidence_rule is not None:
            retry_later, max_attempt, type_incidence_rule = incidence_rule
            contact_history = cls.REDIS_DIALER_CONNECTION.lrange(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}:HISTORY', 0, -1)
            attempt_number = contact_history.count(str((status, status_type)))
            if attempt_number <= max_attempt:
                phone_number = cls.get_next_phone_number(
                    cursor_dialer, id_campaign, contact_id, phone_number)
                retry_later = cls.get_delay(retry_later, type_incidence_rule, attempt_number)
                now_local = datetime.datetime.now()
                datetime_retry_later = now_local + timedelta(seconds=retry_later)

                message = json.dumps({
                    'id_campaign': str(id_campaign),
                    'id_contact': contact_id,
                    'phone_number': phone_number,
                    'datetime_agenda': datetime_retry_later.strftime('%d/%m/%y %H:%M:%S'),
                    'type': 'incidence_rule',
                })
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET final_status = %s '
                    'WHERE id_campaign = %s and id_contact = %s;',
                    (PENDING_ATTEMPTS, id_campaign, contact_id)
                )
                cls.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', PENDING_ATTEMPTS)
                cls.GM_CLIENT.submit_job('schedule-agenda', message)
                return True
            else:
                cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s'
                                      ' WHERE id_campaign = %s and id_contact = %s;',
                                      (FINALIZED_NOCONTACT, id_campaign, contact_id))
                cls.REDIS_DIALER_CONNECTION.hset(
                    f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT)
        else:
            cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                  ' id_campaign = %s and id_contact = %s;',
                                  (FINALIZED_NOCONTACT, id_campaign, contact_id))
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_NOCONTACT)
        return False

    @classmethod
    def handle_incidence_rules(cls, status, id_campaign, contact_id, phone_number):
        # if there is an incidence rule for the status:
        #   if the contact's history and the incidence rule indicates that the
        #   contact must be called again, schedule a call according to the incidence rule
        cls.connect_redis_dialer()
        status_code = NAME_TO_STATUS[status]
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            incidence_rule = cls.get_incidence_rule(id_campaign, status)
            cls.apply_incidence_rule(
                cursor_dialer, incidence_rule, contact_id, id_campaign, status_code,
                PHONE_TYPE, phone_number)

    @classmethod
    @job_handler_decorator
    def process_event(cls, worker, job):
        ari_event_data = cls.decode_payload(job.data)
        event_type = ari_event_data.get('type', 'unknown')
        call_type = ari_event_data.get('call_type', '')
        dialstatus = ari_event_data.get('dialstatus', '')
        dialstring = ari_event_data.get('dialstring', '')
        callid = ari_event_data.get('callid') or ari_event_data.get('uniqueid') or ''

        try:
            id_campaign, contact_id, phone_number = cls.get_contact_data(ari_event_data)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(
                "process_event [%s]: payload sin campos explícitos de negocio (id_campaign/contact_id/phone_number) | type=%s call_type=%s "
                "error=%s payload_keys=%s",
                callid, event_type, call_type, e, list(ari_event_data.keys()) if ari_event_data else [],
            )
            raise

        logger.info(
            "process_event [%s]: recibido | campaign=%s contact=%s phone=%s type=%s call_type=%s "
            "dialstatus=%s dialstring=%s",
            callid, id_campaign, contact_id, phone_number, event_type, call_type, dialstatus, dialstring,
        )

        # ----- RouteValidationFailed (llamada bloqueada por validación de ruta): decrementar -----
        if event_type == 'RouteValidationFailed':
            if int(id_campaign or 0) == 0:
                logger.debug(
                    "process_event [%s]: RouteValidationFailed ignorado (campaña 0)",
                    callid,
                )
                return b'Event was processed'
            cls.connect_redis_dialer()
            key_calls = f'OML:CALLS:{id_campaign}:DIALER'
            try:
                val = cls.REDIS_DIALER_CONNECTION.decr(key_calls)
                if val < 0:
                    cls.REDIS_DIALER_CONNECTION.set(key_calls, 0)
                    logger.warning(
                        "Campaign %s: call count went negative on RouteValidationFailed, reset to 0",
                        id_campaign,
                    )
                cls._publish_calls_count(id_campaign)
            except Exception as e:
                logger.error(
                    "Campaign %s: error decrementing call count on RouteValidationFailed: %s",
                    id_campaign, e,
                    exc_info=True,
                )
            cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
            logger.info(
                "process_event [%s]: RouteValidationFailed, decrement | campaign=%s contact=%s phone=%s",
                callid, id_campaign, contact_id, phone_number,
            )
            return b'Event was processed'

        # ----- ChannelDestroyed (canal PSTN liberado): decrementar y reportar -----
        if event_type in ('ChannelDestroyed', 'ChannelDestroy'):
            if call_type == 'to_pstn' and int(id_campaign or 0) != 0:
                cls.connect_redis_dialer()
                key_calls = f'OML:CALLS:{id_campaign}:DIALER'
                try:
                    val = cls.REDIS_DIALER_CONNECTION.decr(key_calls)
                    if val < 0:
                        cls.REDIS_DIALER_CONNECTION.set(key_calls, 0)
                        logger.warning(
                            "Campaign %s: call count went negative on ChannelDestroyed, reset to 0",
                            id_campaign,
                        )
                    cls._publish_calls_count(id_campaign)
                except Exception as e:
                    logger.error(
                        "Campaign %s: error decrementing call count on ChannelDestroyed: %s",
                        id_campaign, e,
                        exc_info=True,
                    )
                cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
                logger.debug(
                    "process_event [%s]: ChannelDestroyed to_pstn, decrement y send-reports | campaign=%s",
                    callid, id_campaign,
                )
            else:
                logger.debug(
                    "process_event [%s]: ChannelDestroyed ignorado (call_type=%s o campaña 0)",
                    callid, call_type,
                )
            return b'Event was processed'

        # ----- Dial: solo eventos terminales (ANSWER o fallo); intermedios (vacío, RINGING) se ignoran -----
        if event_type != 'Dial':
            logger.debug("process_event [%s]: tipo %s no manejado, omitiendo", callid, event_type)
            return b'Event was processed'

        # Intermedios: no actualizar estado ni enviar send-reports
        if dialstatus in ('', 'RINGING') or dialstatus is None:
            logger.debug(
                "process_event [%s]: Dial intermedio (dialstatus=%r), sin actualizar estado ni send-reports",
                callid, dialstatus,
            )
            return b'Event was processed'

        if cls.is_answer_event(ari_event_data):
            if cls.is_answered_pstn(ari_event_data):
                status = "ANSWERED_PSTN"
                logger.info(
                    "process_event [%s]: ANSWERED_PSTN (troncal) | campaign=%s contact=%s phone=%s",
                    callid, id_campaign, contact_id, phone_number,
                )
                cls.set_contact_status(id_campaign, contact_id, status)
            elif cls.is_answered_agent(ari_event_data):
                status = "ANSWERED_AGENT"
                logger.info(
                    "process_event [%s]: ANSWERED_AGENT (cola) | campaign=%s contact=%s phone=%s",
                    callid, id_campaign, contact_id, phone_number,
                )
                cls.set_contact_status(id_campaign, contact_id, status)
                cls.connect_redis_dialer()
                with cls.get_dialer_connection() as conn_dialer:
                    cursor_dialer = conn_dialer.cursor()
                    cursor_dialer.execute('UPDATE contact_in_campaign SET final_status = %s WHERE'
                                          ' id_campaign = %s and id_contact = %s;',
                                          (FINALIZED_SUCCESS, id_campaign, contact_id))
                    cls.REDIS_DIALER_CONNECTION.hset(
                        f'CONTACT:{contact_id}:CAMP:{id_campaign}', 'STATUS', FINALIZED_SUCCESS)
                logger.info(
                    "process_event [%s]: contacto finalizado con éxito | campaign=%s contact=%s phone=%s",
                    callid, id_campaign, contact_id, phone_number,
                )
        elif cls.is_fail_event(ari_event_data):
            fail_status = cls.decode_fail_event(ari_event_data)
            logger.info(
                "process_event [%s]: evento fallo (Dial) | campaign=%s contact=%s phone=%s dialstatus=%s "
                "decoded=%s",
                callid, id_campaign, contact_id, phone_number, dialstatus, fail_status,
            )
            cls.handle_fail_event(ari_event_data, id_campaign, contact_id, phone_number)
            # CANCEL/AMD/EXIT_SHORTCALL: decrement aquí (ari-app no envía ChannelDestroyed para estos)
            if dialstatus in ('CANCEL', 'AMD', 'EXIT_SHORTCALL') and int(id_campaign or 0) != 0:
                cls.connect_redis_dialer()
                key_calls = f'OML:CALLS:{id_campaign}:DIALER'
                try:
                    val = cls.REDIS_DIALER_CONNECTION.decr(key_calls)
                    if val < 0:
                        cls.REDIS_DIALER_CONNECTION.set(key_calls, 0)
                        logger.warning(
                            "Campaign %s: call count went negative on Dial %s, reset to 0",
                            id_campaign, dialstatus,
                        )
                    cls._publish_calls_count(id_campaign)
                except Exception as e:
                    logger.error(
                        "Campaign %s: error decrementing call count on Dial %s: %s",
                        id_campaign, dialstatus, e,
                        exc_info=True,
                    )
                logger.debug(
                    "process_event [%s]: Dial %s, decrement | campaign=%s",
                    callid, dialstatus, id_campaign,
                )
        else:
            logger.debug(
                "process_event [%s]: Dial no answer ni fail (ignorado) | campaign=%s contact=%s dialstatus=%s",
                callid, id_campaign, contact_id, dialstatus,
            )

        cls.GM_CLIENT.submit_job('send-reports', job.data, background=True)
        logger.debug(
            "process_event [%s]: finalizado, job send-reports enviado | campaign=%s contact=%s",
            callid, id_campaign, contact_id,
        )
        return b'Event was processed'

    @classmethod
    def is_fail_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        type_event = ari_event_data.get('type')
        return type_event == 'Dial' and  \
            ((dialstatus in FAIL_EVENTS) or (dialstatus in FAIL_NO_RULES_EVENTS))

    @classmethod
    def decode_fail_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        if dialstatus != "NOANSWER":
            return dialstatus
        dialstring = ari_event_data.get('dialstring')
        pattern_timeout = r'^camp_\d+@omlacd$'
        if re.match(pattern_timeout, dialstring):
            return "TIMEOUT"
        return "NOANSWER"

    @classmethod
    def handle_fail_event(cls, ari_event_data, id_campaign, contact_id, phone_number):
        event = cls.decode_fail_event(ari_event_data)
        logger.debug(f'Campaign {id_campaign}: receiving {event} for contact {contact_id}')
        cls.set_contact_status(id_campaign, contact_id, event)
        if event in FAIL_EVENTS:
            cls.handle_incidence_rules(event, id_campaign, contact_id, phone_number)

    @classmethod
    def is_answer_event(cls, ari_event_data):
        dialstatus = ari_event_data.get('dialstatus')
        type_event = ari_event_data.get('type')
        return type_event == 'Dial' and dialstatus == 'ANSWER'

    @classmethod
    def get_contact_data(cls, ari_event_data):
        """
        Extrae id_campaign, contact_id y phone_number desde campos explícitos del payload.

        Contrato requerido:
        - id_campaign
        - contact_id
        - phone_number
        """
        required_fields = ('id_campaign', 'contact_id', 'phone_number')
        missing_fields = [field for field in required_fields if field not in ari_event_data]
        if missing_fields:
            raise KeyError(f"Missing required fields: {missing_fields}")

        id_campaign = ari_event_data.get('id_campaign')
        contact_id = ari_event_data.get('contact_id')
        phone_number = ari_event_data.get('phone_number')

        if id_campaign in (None, "") or contact_id in (None, "") or phone_number in (None, ""):
            raise ValueError(
                "Invalid empty values in required fields: "
                f"id_campaign={id_campaign!r}, contact_id={contact_id!r}, phone_number={phone_number!r}"
            )

        return str(id_campaign), str(contact_id), str(phone_number)

    @classmethod
    def is_answered_pstn(cls, ari_event_data):
        """
        Indica si la respuesta fue PSTN (troncal).
        Usa call_type cuando el ACD lo envía (to_pstn / to_agent); fallback a dialstring.
        """
        call_type = ari_event_data.get('call_type')
        if call_type == 'to_pstn':
            return True
        if call_type == 'to_agent':
            return False
        dialstring = ari_event_data.get('dialstring') or ''
        return dialstring.find('camp_') == -1

    @classmethod
    def is_answered_agent(cls, ari_event_data):
        """
        Indica si la respuesta fue agente (cola).
        Usa call_type cuando el ACD lo envía; fallback a dialstring.
        """
        call_type = ari_event_data.get('call_type')
        if call_type == 'to_agent':
            return True
        if call_type == 'to_pstn':
            return False
        dialstring = ari_event_data.get('dialstring') or ''
        return dialstring.find('camp_') >= 0

    @classmethod
    def set_contact_status(cls, id_campaign, contact_id, status, type_status=PHONE_TYPE):
        # Campaña 0 o contacto 0: ignorar eventos de estado (casos de sistema)
        if int(id_campaign) == 0 or int(contact_id or 0) == 0:
            logger.info(f"SYSTEM EVENT: Status {status} ignored for ID 0")
            return
        status_code = NAME_TO_STATUS[status]
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if status_code == STATUS_ANSWERED_PSTN:
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status_pstn = true, '
                    'history = array_append(history, %s)'
                    ' WHERE id_campaign = %s AND id_contact = %s;',
                    (str((status_code, type_status)), id_campaign, contact_id))
            else:
                cursor_dialer.execute(
                    'UPDATE contact_in_campaign SET status = %s, '
                    'history = array_append(history, %s)'
                    ' WHERE id_campaign = %s AND id_contact = %s;',
                    (status_code, str((status_code, type_status)), id_campaign, contact_id))
            cls.connect_redis_dialer()
            cls.REDIS_DIALER_CONNECTION.rpush(
                f'CONTACT:{contact_id}:CAMP:{id_campaign}:HISTORY', str((status_code, type_status)))
            cls.REDIS_DIALER_CONNECTION.hincrby(
                f'CAMP:{id_campaign}:COUNTER',
                status
            )

    @classmethod
    @job_handler_decorator
    def delete_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        logger.debug(f'Removing campaign with id = {id_campaign}')
        cls.connect_redis_dialer()
        cls.connect_redis_oml()
        was_in_set = cls.REDIS_DIALER_CONNECTION.srem(cls.ACTIVE_CAMPAIGNS_SET, id_campaign)
        if was_in_set:
            cls.update_percentages_priority_campaigns()
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM campaign WHERE id = %s;', (id_campaign,))
            cls.REDIS_DIALER_CONNECTION.delete(f'OML:CALLS:{id_campaign}:DIALER')
            cls.update_percentages_priority_campaigns()
            # TODO: remove the remaining data in Redis
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({'type': 'DELETE',
                            'camp_id': id_campaign}))
        try:
            cls.get_boost_factor.cache_clear()
            cls.get_campaign_max_available_channels.cache_clear()
            cls.get_incidence_rule.cache_clear()
            cls.get_incidence_rule_disposition.cache_clear()
            cls.get_prefix.cache_clear()
        except AttributeError:
            pass
        return b'Campaign was deleted'

    @classmethod
    def set_campaign_status(cls, id_campaign, new_status, cursor=None, sync_omnileads=False):
        """
        Centralized method for changing a campaign's state.
        It updates the DB, the state in Redis, and triggers the global percentage recalculation.
        """
        is_now_active = (new_status == ACTIVE)

        # 1. Upgrade DB
        if not sync_omnileads:
            if cursor is None:
                with cls.get_dialer_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'UPDATE campaign SET '
                        'dialer_status = %s WHERE id = %s;', (new_status, id_campaign)
                    )
            else:
                cursor.execute(
                    'UPDATE campaign SET '
                    'dialer_status = %s WHERE id = %s;', (new_status, id_campaign)
                )
        else:
            with cls.get_dialer_connection() as conn_dialer, conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'UPDATE campaign SET '
                        'dialer_status = %s WHERE id = %s;', (new_status, id_campaign)
                    )
                    cursor_oml.execute(
                        'UPDATE ominicontacto_app_campana '
                        'SET estado = %s WHERE id = %s;', (new_status, id_campaign)
                    )

        # 2. Update Redis status directly
        cls.connect_redis_dialer()
        redis_key = f'CAMP:{id_campaign}:DISTRIBUTION'
        cls.REDIS_DIALER_CONNECTION.hset(redis_key, 'STATUS', '1' if is_now_active else '0')
        if is_now_active:
            cls.REDIS_DIALER_CONNECTION.sadd(cls.ACTIVE_CAMPAIGNS_SET, id_campaign)
        else:
            cls.REDIS_DIALER_CONNECTION.srem(cls.ACTIVE_CAMPAIGNS_SET, id_campaign)

        # 3. Trigger global recalculation. This function will handle any necessary backfill.
        cls.update_percentages_priority_campaigns()

        # 4. Publish the change (logic unchanged)
        cls.connect_redis_oml()
        cls.REDIS_OML_CONNECTION.publish(
            "OML:CHANNEL:DIALER",
            json.dumps({'type': 'STATUSCHANGE',
                        'camp_id': id_campaign,
                        'status': new_status,
                        'admin': AdminRender.render_status_change(
                            id_campaign, new_status, CAMPAIGN_STATUS_TO_NAME[new_status],
                            AVAILABLE_NEXT_STATUSES[new_status])}))

    @classmethod
    @job_handler_decorator
    def stop_campaign(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        sync_omnileads = data['sync_omnileads']
        logger.debug(f'Stopping campaign with id = {id_campaign}')
        cls.set_campaign_status(id_campaign, FINALIZED, sync_omnileads=sync_omnileads)
        return b'Campaign was finalized'

    @classmethod
    def update_prev_stats(cls, id_campaign):
        """Copy the current stats to the key 'COUNTER_PREV' so it can be used for comparison
        and send only the modified keys"""
        for key, value in cls.REDIS_DIALER_CONNECTION.hgetall(
                f'CAMP:{id_campaign}:COUNTER').items():
            cls.REDIS_DIALER_CONNECTION.hset(f'CAMP:{id_campaign}:COUNTER_PREV', key, value)

    @classmethod
    @job_handler_decorator
    def send_reports(cls, worker, job):
        cls.connect_redis_dialer()
        ari_event_data = cls.decode_payload(job.data)
        id_campaign, contact_id, phone_number = cls.get_contact_data(ari_event_data)

        # GUARDIA: Si es campaña de sistema (ID 0), no generamos reportes de base de datos
        if int(id_campaign) == 0:
            return b'Success: System campaign reports skipped'

        previous_stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER_PREV') \
            or {}
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """SELECT COUNT(*) FROM ONLY contact_in_campaign WHERE id_campaign = %s
                and (status = %s or status = %s);""",
                (id_campaign, STATUS_CREATED, STATUS_SELECTED_CALL))
            pending_for_call = cursor_dialer.fetchone()[0]
            cursor_dialer.execute("""SELECT final_status, COUNT(*) FROM ONLY contact_in_campaign
            WHERE id_campaign = %s and final_status <> %s GROUP BY final_status;""",
                                  (id_campaign, INITIAL))

            final_statuses = {
                PENDING_ATTEMPTS: 0,
                FINALIZED_NOCONTACT: 0,
                FINALIZED_SUCCESS: 0
            }
            for final_status_label, final_status_value in cursor_dialer.fetchall():
                final_statuses[final_status_label] = final_status_value

            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[PENDING_ATTEMPTS],
                final_statuses[PENDING_ATTEMPTS]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[FINALIZED_NOCONTACT],
                final_statuses[FINALIZED_NOCONTACT]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                FINAL_STATUS_TO_NAME[FINALIZED_SUCCESS],
                final_statuses[FINALIZED_SUCCESS]
            )
            cls.REDIS_DIALER_CONNECTION.hset(
                f'CAMP:{id_campaign}:COUNTER',
                'PENDING_INITIAL_CONTACT_ATTEMPTS',  # pending to be contacted for the first time
                pending_for_call
            )
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            logger.debug(f'Report for campaign {id_campaign}: {stats}')
            cls.connect_redis_oml()
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                json.dumps({
                    'type': 'EVENT',
                    'camp_id': id_campaign,
                    'data': ari_event_data
                }))
            stats_message = {
                'type': 'STATS',
                'camp_id': id_campaign,
            }
            # for Redis PUBSUB
            changed_stats = dict(set(stats.items()) - set(previous_stats.items()))
            changed_stats.update(stats_message)
            changed_stats.update({'admin': AdminRender.render_stats_inner(
                id_campaign, stats)})
            changed_stats_json = json.dumps(changed_stats)
            cls.REDIS_OML_CONNECTION.publish(
                'OML:CHANNEL:DIALER',
                changed_stats_json)
            cls.update_prev_stats(id_campaign)
            # for Postgres in Omnidialer
            stats_json = json.dumps(stats)
            cursor_dialer.execute(
                'UPDATE campaign SET statistics = %s WHERE id = %s;', (stats_json, id_campaign))
            return b'Success!'

    @classmethod
    def handle_disposition_option(cls, data):
        id_campaign = data['id_campaign']
        id_contact = data['id_contact']
        disposition_option = data['disposition_option']
        logger.debug(f'Adding disposition option {disposition_option} to contact {id_contact}'
                     f' in campaign {id_campaign}')
        cls.connect_redis_dialer()
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                """UPDATE contact_in_campaign as cc
                SET disposition_option = %s, history = array_append(history, %s)
                FROM contact AS co
                WHERE cc.id_campaign = %s AND cc.id_contact = %s AND co.id = cc.id_contact
                RETURNING co.phone;""",
                (disposition_option, str((disposition_option, DISPOSITION_TYPE)),
                 id_campaign, id_contact))
            phone_number = cursor_dialer.fetchone()[0]
            cls.REDIS_DIALER_CONNECTION.rpush(
                f'CONTACT:{id_contact}:CAMP:{id_campaign}:HISTORY',
                str((disposition_option, DISPOSITION_TYPE)))
            cls.connect_redis_dialer()
            incidence_rule = cls.get_incidence_rule_disposition(id_campaign, disposition_option)
            incidence_rule_applied = cls.apply_incidence_rule(
                cursor_dialer, incidence_rule, id_contact, id_campaign, disposition_option,
                DISPOSITION_TYPE, phone_number)
            # if the incidence rule was applied and the campaign is paused, reactivate the campaign
            if incidence_rule_applied:
                status = cls.get_campaign_status(id_campaign, cursor_dialer)
                if status == PAUSED:
                    cls.set_campaign_status(id_campaign, ACTIVE, cursor=cursor_dialer,
                                            sync_omnileads=True)
                    message = json.dumps({'id_campaign': id_campaign})
                    cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
            return b'Disposition for incidence rule was added!'

    @classmethod
    def audit_active_channels(cls):
        """
        Sincroniza el conteo de canales activos con Redis.
        La fuente de verdad de canales queda en el ACD/Gearman; este método está preparado
        para recibir en el futuro una lista de canales (p. ej. vía API o job) y actualizar Redis.
        """
        logger.info("Iniciando auditoría de canales activos (Sanity Check)...")
        
        try:
            # Fuente de canales: en el futuro puede venir de ACD/Gearman o API
            channels = []  # Sin conexión ARI directa; rellenar cuando exista integración

            # 2. Mapear canales por id_campaign
            # El caller_id tiene formato: {id_campaign}_{id_customer}_{phone_number}
            actual_counts = {}
            
            for chan in channels:
                caller_name = chan.get('caller', {}).get('name', '')
                if not caller_name:
                    continue
                
                parts = caller_name.split('_')
                if len(parts) >= 2:
                    try:
                        camp_id = int(parts[0])
                        actual_counts[camp_id] = actual_counts.get(camp_id, 0) + 1
                    except ValueError:
                        continue

            # 3. Obtener todas las campañas que Redis cree que tienen llamadas
            cls.connect_redis_dialer()
            # Buscamos todas las llaves OML:CALLS:*:DIALER
            redis_keys = cls.REDIS_DIALER_CONNECTION.keys('OML:CALLS:*:DIALER')
            
            pipe = cls.REDIS_DIALER_CONNECTION.pipeline()

            # 4. Sincronizar campañas activas según conteo de canales
            for camp_id, count in actual_counts.items():
                key = f'OML:CALLS:{camp_id}:DIALER'
                logger.debug(f"Audit: Campaña {camp_id} tiene {count} canales reales.")
                pipe.set(key, count)

            # 5. Limpiar campañas que Redis cree que tienen llamadas pero el conteo real es 0
            for key in redis_keys:
                try:
                    # Extraer ID de la campaña de la llave 'OML:CALLS:{id}:DIALER'
                    camp_id_in_redis = int(key.split(':')[2])
                    if camp_id_in_redis not in actual_counts:
                        logger.warning(f"Audit: Campaña {camp_id_in_redis} en Redis tiene llamadas huérfanas. Reseteando a 0.")
                        pipe.set(key, 0)
                except (IndexError, ValueError):
                    continue

            pipe.execute()
            logger.info("Auditoría completada exitosamente.")

        except Exception as e:
            logger.error(f"Error crítico en audit_active_channels: {e}", exc_info=True)

    @classmethod
    def handle_amd_option(cls, data):
        event = 'AMD'  # entidad propia: history (10, 1) y métricas "AMD Detected"
        id_campaign = data['id_campaign']
        id_contact = data['id_contact']
        phone_number = data['phone_number']
        logger.debug(f'Campaign {id_campaign}: receiving {event} for contact {id_contact}')
        cls.set_contact_status(id_campaign, id_contact, event)
        cls.handle_incidence_rules(event, id_campaign, id_contact, phone_number)
        return b'Disposition for AMD was handled'

    @classmethod
    @job_handler_decorator
    def add_incidence_rule_disposition(cls, worker, job):
        data = cls.decode_payload(job.data)
        disposition_option = data['disposition_option']
        if disposition_option == -2:
            return cls.handle_amd_option(data)
        return cls.handle_disposition_option(data)

    @classmethod
    @job_handler_decorator
    def create_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        status_custom = data['status_custom']
        max_attempt = data['max_attempt']
        retry_later = data['retry_later']
        mode = data['mode']
        STATUS = 1
        logger.debug(f'Adding incidence rule to campaign {id_campaign}')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            if type_rule == STATUS:
                status = data['status']
                cursor_dialer.execute(
                    """INSERT INTO incidence_rules (id, status, status_custom, max_attempt,
                    retry_later, in_mode, campaign_id) VALUES
                    (%s, %s, %s, %s, %s, %s, %s);""",
                    (id_rule, status, status_custom, max_attempt, retry_later, mode, id_campaign))
            else:
                disposition_option_id = data['disposition_option_id']
                cursor_dialer.execute(
                    """INSERT INTO incidence_rules_disposition (id, disposition_option_id,
                    max_attempt, retry_later, in_mode, campaign_id) VALUES
                    (%s, %s, %s, %s, %s, %s);""",
                    (id_rule, disposition_option_id, max_attempt, retry_later, mode, id_campaign))
            return b'Incidence rule was added'

    @classmethod
    @job_handler_decorator
    def delete_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']
        type_rules = ['STATUS', 'DISPOSITION']
        type_rule_label = type_rules[type_rule - 1]
        STATUS = 1
        logger.debug(f'Removing incidence_rule {id_rule} of type {type_rule_label} '
                     f'in campaign with id = {id_campaign}')
        with cls.get_dialer_connection() as conn:
            cursor = conn.cursor()
            if type_rule == STATUS:
                cursor.execute('DELETE FROM incidence_rules WHERE id = %s;', (id_rule,))
            else:
                cursor.execute('DELETE FROM incidence_rules_disposition WHERE id = %s;', (id_rule,))
            return b'Incidence rule was deleted'

    @classmethod
    @job_handler_decorator
    def update_incidence_rule(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        id_rule = data['id_rule']
        type_rule = data['type_rule']  # 1 = STATUS, 2 = DISPOSITION

        # comunes
        max_attempt = data['max_attempt']
        retry_later = data['retry_later']
        mode = data['mode']

        STATUS = 1
        logger.debug(
            f'Updating incidence rule {id_rule} (type={type_rule}) in campaign {id_campaign}'
        )

        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()

            if type_rule == STATUS:
                status = data['status']
                status_custom = data['status_custom']
                cursor_dialer.execute(
                    """UPDATE incidence_rules
                    SET status = %s,
                        status_custom = %s,
                        max_attempt = %s,
                        retry_later = %s,
                        in_mode = %s,
                        campaign_id = %s
                    WHERE id = %s;""",
                    (status, status_custom, max_attempt, retry_later, mode, id_campaign, id_rule)
                )
            else:
                disposition_option_id = data['disposition_option_id']
                cursor_dialer.execute(
                    """UPDATE incidence_rules_disposition
                    SET disposition_option_id = %s,
                        max_attempt = %s,
                        retry_later = %s,
                        in_mode = %s,
                        campaign_id = %s
                    WHERE id = %s;""",
                    (disposition_option_id, max_attempt, retry_later, mode, id_campaign, id_rule)
                )

        try:
            cls.get_incidence_rule.cache_clear()
            cls.get_incidence_rule_disposition.cache_clear()
        except AttributeError:
            pass

        return b'Incidence rule was updated'

    @classmethod
    @job_handler_decorator
    def change_database(cls, worker, job):
        data = cls.decode_payload(job.data)
        id_campaign = data['id_campaign']
        # 0- Pause the campaign
        cls.set_campaign_status(id_campaign, PAUSED, sync_omnileads=True)
        # 1- remove Redis related reports & contacts history
        AverageWorker.connect_redis_dialer()
        AverageWorker.REDIS_DIALER_CONNECTION.delete(f'CAMP:{id_campaign}:COUNTER')
        for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                match=f'CONTACT:*:CAMP:{id_campaign}', count=1000):
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key)
        for key in cls.REDIS_DIALER_CONNECTION.scan_iter(
                match=f'CONTACT:*:CAMP:{id_campaign}:HISTORY', count=1000):
            AverageWorker.REDIS_DIALER_CONNECTION.delete(key)
        # 2- remove Postgres related reports & contacts history
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'DELETE FROM contact_in_campaign WHERE id_campaign = %s', (id_campaign,))
                    cursor_dialer.execute(
                        'UPDATE campaign SET statistics = NULL WHERE id = %s', (id_campaign,))
                    # 3- bring the new contacts from OML
                    cls.copy_contacts_from_oml(cursor_dialer, cursor_oml, id_campaign)
        return b'Database was updated'

    @classmethod
    @job_handler_decorator
    def render_template(cls, worker, job):
        # Job dedicated to HTMX rendering
        data = cls.decode_payload(job.data)
        if data['type'] == 'init':
            logger.debug('HTMX related: getting the information of campaigns for the first time')
            with cls.get_dialer_connection() as conn_dialer:
                cursor_dialer = conn_dialer.cursor()
                cursor_dialer.execute('SELECT id, name, dialer_status from campaign;')
                campaigns = cursor_dialer.fetchall()
                # transforming dialer_status value to a label
                campaigns = [(id_camp, name, CAMPAIGN_STATUS_TO_NAME[status],
                              AVAILABLE_NEXT_STATUSES[status])
                             for (id_camp, name, status) in campaigns]
                cursor_dialer.execute('SELECT is_active FROM system_control;')
                running = cursor_dialer.fetchone()[0]
                return AdminRender.render_init(campaigns, running)
        if data['type'] == 'stats':
            id_campaign = data['id_campaign']
            cls.connect_redis_dialer()
            stats = cls.REDIS_DIALER_CONNECTION.hgetall(f'CAMP:{id_campaign}:COUNTER')
            return AdminRender.render_stats(id_campaign, stats)

    @classmethod
    def stop_dialer(cls):
        logger.debug('Stopping dialer')
        with cls.get_dialer_connection() as conn_dialer:
            with conn_dialer.transaction():
                cursor_dialer = conn_dialer.cursor()
                with cls.get_oml_connection() as conn_oml:
                    cursor_oml = conn_oml.cursor()
                    cursor_dialer.execute(
                        'UPDATE system_control '
                        'SET is_active = false, updated_at = now() WHERE id = true;'
                    )
                    logger.debug('Pausing all active campaigns')
                    cursor_dialer.execute(
                        'UPDATE campaign '
                        'SET dialer_status = %s WHERE dialer_status = %s RETURNING id;',
                        (PAUSED, ACTIVE)
                    )
                    cursor_oml.execute(
                        'UPDATE ominicontacto_app_campana '
                        'SET estado = %s WHERE estado = %s;',
                        (PAUSED, ACTIVE)
                    )

        # **mantener Redis en sync** después del bulk
        cls.rebuild_active_campaigns_set()
        cls.connect_redis_oml()
        cls.REDIS_OML_CONNECTION.publish(
            "OML:CHANNEL:DIALER",
            json.dumps({'type': 'PAUSE_BULK', 'camp_id': 'all'})
        )

    @classmethod
    def start_dialer(cls):
        logger.debug('Starting dialer')
        with cls.get_dialer_connection() as conn_dialer:
            cursor_dialer = conn_dialer.cursor()
            cursor_dialer.execute(
                'UPDATE system_control SET is_active = true, updated_at = now() WHERE id = true;')
        # mantener Redis en sync
        cls.rebuild_active_campaigns_set()
        cls.update_percentages_priority_campaigns()

    @classmethod
    def handle_dialer_action(cls, action):
        if action == 'start':
            cls.start_dialer()
        elif action == 'stop':
            cls.stop_dialer()
        else:
            cls.stop_dialer()
            cls.start_dialer()

    @classmethod
    @job_handler_decorator
    def manage_dialer(cls, worker, job):
        data = cls.decode_payload(job.data)
        action = data['action']
        cls.handle_dialer_action(action)
        cls.connect_redis_oml()
        cls.REDIS_OML_CONNECTION.publish(
            'OML:CHANNEL:DIALER',
            json.dumps({'type': 'DIALER_STATUS_CHANGE',
                        'action': action,
                        'camp_id': 'all'})
        )
        running = True
        if action == "stop":
            running = False
        return AdminRender.render_status_dialer(running)


class SchedulerWorker(AverageWorker):
    """
    Worker del scheduler (naive datetimes, sin TZ/UTC).
    - Usa jobstore Redis (db configurable, por defecto 3).
    - Decrementa agendas de forma confiable e idempotente ante ADDED/EXECUTED/ERROR/REMOVED/MISSED.
    - Evita doble incremento cuando se reemplaza un job existente.
    """

    # ---------- Infra del scheduler ----------
    EXECUTORS = {'default': ThreadPoolExecutor(1)}
    JOB_DEFAULTS = {'misfire_grace_time': 3600, 'coalesce': True, 'max_instances': 1}
    JOBSTORES = {
        'default': RedisJobStore(
            host=REDIS_DIALER_SERVER,
            port=int(REDIS_DIALER_PORT),
            db=REDIS_DIALER_DB,
            jobs_key='apscheduler.jobs',
            run_times_key='apscheduler.run_times',
            # password=os.getenv('REDIS_DIALER_PASSWORD')
        )
    }
    SCHEDULER = BackgroundScheduler(
        executors=EXECUTORS,
        job_defaults=JOB_DEFAULTS,
        jobstores=JOBSTORES,
    )

    # ---------- Redis auxiliar (para "seen set" y fallback) ----------
    @classmethod
    def _redis(cls):
        return redis.StrictRedis(
            host=REDIS_DIALER_SERVER,
            port=int(REDIS_DIALER_PORT),
            db=REDIS_DIALER_DB,
            decode_responses=True,
        )

    @staticmethod
    def _camp_seen_key(camp_id: int) -> str:
        return f"CAMP:{camp_id}:SCHED:SEEN"

    # ---------- Helpers de extracción ----------
    @classmethod
    def _extract_campaign_id_from_job_or_id(cls, job, job_id: str | None = None) -> int | None:
        """
        Extrae id_campaign desde:
        1) name/id del objeto job (name *_<id>_* ó *_<id>)
        2) args/kwargs del job
        3) fallback: patrón en job_id de APScheduler (p.ej. 'agenda_contact:{id}:{...}')
        """
        try:
            name = (getattr(job, 'name', '') or '') + '_' + (getattr(job, 'id', '') or '')
            m = re.search(r'_(\d+)(?:_|$)', name)
            if m:
                return int(m.group(1))

            # Fallback: args / kwargs
            args = getattr(job, 'args', ()) or ()
            for a in args:
                if isinstance(a, int):
                    return a
                if isinstance(a, str) and a.isdigit():
                    return int(a)

            kwargs = getattr(job, 'kwargs', {}) or {}
            for v in kwargs.values():
                if isinstance(v, int):
                    return v
                if isinstance(v, str) and v.isdigit():
                    return int(v)
            # 3) Fallback: parsear job_id del evento (agenda_contact:9:104 / process_campaign:9:...)
            if job_id:
                m2 = re.search(r':(\d+)(?::|$)', job_id)
                if m2:
                    return int(m2.group(1))
        except Exception as e:
            logger.debug("extract_campaign_id_from_job failed: %s", e)
        return None

    # ---------- Decremento seguro e idempotente ----------
    @classmethod
    def _agendas_decrement_once(cls, camp_id: int, job_id: str):
        """
        Decrementa una sola vez por job_id usando un set "seen".
        Si AverageWorker.agendas_decrement_safe existe, lo usa.
        Si no, hace SADD y luego agendas_decrement normal.
        """
        # Si el proyecto ya define un método seguro, úsalo
        if hasattr(AverageWorker, 'agendas_decrement_safe'):
            try:
                AverageWorker.agendas_decrement_safe(camp_id, job_id)
                return
            except Exception as e:
                logger.warning("agendas_decrement_safe falló, fallback a seen set: %s", e)

        # Fallback con seen set
        r = cls._redis()
        seen_key = cls._camp_seen_key(camp_id)
        try:
            added = r.sadd(seen_key, job_id)  # 1 si no existía
            if added == 1:
                r.expire(seen_key, AverageWorker.SEEN_TTL_SECONDS)
                AverageWorker.agendas_decrement(camp_id, 1)
        except Exception as e:
            logger.error("Fallo decremento seen-set para camp %s job %s: %s", camp_id, job_id, e)

    # ---------- Listener de eventos + logs ----------
    @staticmethod
    def _event_name(mask: int) -> str:
        if mask == EVENT_JOB_ADDED:
            return "ADDED"
        if mask == EVENT_JOB_REMOVED:
            return "REMOVED"
        if mask == EVENT_JOB_EXECUTED:
            return "EXECUTED"
        if mask == EVENT_JOB_ERROR:
            return "ERROR"
        if mask == EVENT_JOB_MISSED:
            return "MISSED"
        return f"UNKNOWN({mask})"

    @classmethod
    def _log_job_add(cls, *, job_id: str, name: str, run_date, exists_before: bool):
        try:
            jobstore_aliases = ",".join(cls.SCHEDULER._jobstores.keys())  # pragma: no cover
        except Exception:
            jobstore_aliases = "unknown"

        logger.info(
            "SCHED ADD %s | name=%s run_date=%s jobstores=%s replaced=%s",
            job_id, name, run_date, jobstore_aliases, exists_before,
        )

    @classmethod
    def _replacing_key(cls, job_id: str) -> str:
        return f"SCHED:REPLACING:{job_id}"

    @classmethod
    def _is_replacing(cls, job_id: str) -> bool:
        """True si el job_id está siendo reemplazado (replace_existing)."""
        try:
            return bool(cls._redis().get(cls._replacing_key(job_id)))
        except Exception:
            logger.debug("No se pudo leer replacing flag para %s", job_id, exc_info=True)
            return False

    @classmethod
    def _agenda_decrement_listener(cls, event):
        """Listener unificado: log + decremento idempotente (y safe ante replace_existing)."""
        try:
            jobstore = getattr(event, "jobstore", "unknown")
            evt = cls._event_name(event.code)

            # IMPORTANTE: en REMOVED el job puede ya no existir en el scheduler.
            job = None
            try:
                job = cls.SCHEDULER.get_job(event.job_id)
            except Exception:
                job = None

            camp_id = cls._extract_campaign_id_from_job_or_id(
                job, getattr(event, "job_id", None)
            )

            logger.info(
                "SCHED EVT %s | job_id=%s jobstore=%s campaign_id=%s",
                evt, event.job_id, jobstore, camp_id,
            )

            # (1) Si es REMOVED por replace_existing => NO decrementar.
            if event.code == EVENT_JOB_REMOVED:
                if cls._is_replacing(event.job_id):
                    logger.info(
                        "SCHED EVT REMOVED por replace_existing (skip decrement) job_id=%s",
                        event.job_id
                    )
                    return

            # (2) Decremento idempotente en eventos terminales
            if (
                event.code in (
                    EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_REMOVED,
                    EVENT_JOB_MISSED
                )
                and camp_id is not None
            ):
                cls._agendas_decrement_once(camp_id, event.job_id)

        except Exception as e:
            logger.warning("Agenda decrement/log listener failed: %s", e, exc_info=True)

    @classmethod
    def _register_listener_once(cls):
        """Registra el listener una sola vez (alta/baja/exec/error/missed)."""
        global _SCHED_LISTENER_REGISTERED
        if _SCHED_LISTENER_REGISTERED:
            return
        cls.SCHEDULER.add_listener(
            cls._agenda_decrement_listener,
            EVENT_JOB_ADDED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR |
            EVENT_JOB_REMOVED | EVENT_JOB_MISSED,
        )
        _SCHED_LISTENER_REGISTERED = True
        logger.debug("Scheduler listener registrado para ADDED/EXECUTED/ERROR/REMOVED/MISSED")

    # ---------- Funciones ejecutadas por los jobs ----------
    @classmethod
    def schedule_contact(cls, phone_number, id_campaign, id_contact):
        logger.debug(f'Campaign {id_campaign}: firing scheduled agenda for contact {id_contact}')
        # Enviar llamada a través de ACD usando Gearman
        success = cls.trigger_acd_dial(
            phone_number=phone_number,
            campaign_id=id_campaign,
            contact_id=id_contact
        )
        if success:
            return 'GD!!!'
        else:
            logger.warning(
                f'Campaign {id_campaign}: Failed to send dial job for contact {id_contact}'
            )
            return 'Error sending dial job'

    @classmethod
    def schedule_process_campaign(cls, id_campaign):
        logger.debug(f'Campaign {id_campaign} starting to run from the scheduler')
        message = json.dumps({'id_campaign': id_campaign})
        cls.GM_CLIENT.submit_job('process-campaign', message, background=True)
        return 'GD!!!'

    # ---------- API de agenda (entrypoint Gearman) ----------
    @classmethod
    @job_handler_decorator
    def schedule_agenda(cls, worker, job):
        cls._register_listener_once()
        if not cls.SCHEDULER.running:
            cls.SCHEDULER.start(paused=False)

        data = cls.decode_payload(job.data)
        id_campaign = int(data['id_campaign'])
        schedule_type = data.get('type', 'agenda')

        # Parser naive (sin TZ)
        def _parse_naive(dt_str: str) -> datetime.datetime:
            return datetime.datetime.strptime(dt_str, '%d/%m/%y %H:%M:%S')

        # Helper: limpiar seen siempre (corrige punto 2)
        def _clear_seen(job_id: str):
            try:
                cls._redis().srem(cls._camp_seen_key(id_campaign), job_id)
            except Exception as e:
                logger.warning("No se pudo limpiar seen set para %s: %s", job_id, e)

        # Helper: marcar reemplazo SIEMPRE (robusto ante falsos negativos de exists)
        def _mark_replacing(job_id: str):
            try:
                # TTL corto: cubre la ventana de REMOVED->ADDED durante replace_existing
                cls._redis().setex(cls._replacing_key(job_id), 5, "1")
            except Exception:
                logger.debug("No se pudo setear replacing flag para %s", job_id, exc_info=True)

        if schedule_type == 'process-campaign':
            datetime_start_str = data.get('datetime_start', '')
            if not datetime_start_str:
                return b'Missing datetime_start'

            run_date = _parse_naive(datetime_start_str)

            # ID único determinístico: permite reprogramar sin doble incrementar
            job_id = f'process_campaign:{id_campaign}:{run_date.strftime("%Y%m%d%H%M%S")}'
            name = f'scheduled_process_campaign_{id_campaign}'

            # exists se usa SOLO para decidir incremento; no para replacing
            exists = cls.SCHEDULER.get_job(job_id) is not None

            # replace_existing=True => marcamos replacing siempre
            _mark_replacing(job_id)

            cls.SCHEDULER.add_job(
                cls.schedule_process_campaign,
                trigger='date',
                run_date=run_date,
                args=[id_campaign],
                id=job_id,
                name=name,
                replace_existing=True,
                misfire_grace_time=3600,
                coalesce=True,
                max_instances=1,
            )

            cls._log_job_add(job_id=job_id, name=name, run_date=run_date, exists_before=exists)

            # Limpiar seen siempre al agendar (exista o no)
            _clear_seen(job_id)

            # Incremento sólo si no existía (no duplicar agendas)
            if not exists:
                AverageWorker.agendas_increment(id_campaign, 1)

            return b'Campaign process was scheduled'

        # Agenda de contacto
        datetime_agenda_str = data.get('datetime_agenda', '')
        if not datetime_agenda_str:
            return b'Missing datetime_agenda'

        run_date = _parse_naive(datetime_agenda_str)
        id_contact = data.get('id_contact')
        phone_number = data.get('phone_number', '')
        if not id_contact:
            job_id = (
                f'agenda_contact:{id_campaign}:{phone_number}:'
                f'{run_date.strftime("%Y%m%d%H%M%S")}'
            )
        else:
            job_id = f'agenda_contact:{id_campaign}:{id_contact}'

        name = f'agenda_contact_{id_campaign}_{id_contact}'

        exists = cls.SCHEDULER.get_job(job_id) is not None

        # replace_existing=True => marcamos replacing siempre
        _mark_replacing(job_id)

        cls.SCHEDULER.add_job(
            cls.schedule_contact,
            trigger='date',
            run_date=run_date,
            args=[phone_number, id_campaign, id_contact],
            id=job_id,
            name=name,
            replace_existing=True,
            misfire_grace_time=3600,
            coalesce=True,
            max_instances=1,
        )

        cls._log_job_add(job_id=job_id, name=name, run_date=run_date, exists_before=exists)

        _clear_seen(job_id)

        if not exists:
            AverageWorker.agendas_increment(id_campaign, 1)

        return b'Agenda was scheduled'
