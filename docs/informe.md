# Informe Técnico — OmniDialer (Gearman/Asterisk)

**Fecha:** 2026-06-24  
**Revisado por:** Análisis estático del repositorio `components-git-repo/dialer`  
**Rama analizada:** estado actual del working tree

---

## 1. Resumen Ejecutivo

El dialer es un orquestador de campañas outbound para Asterisk/OML, compuesto por:

- **API Flask** (`interface/src/app.py`) — recibe comandos HTTP y los encola en Gearman.
- **Worker Gearman** (`workers/handle-campaign/src/handler/naive.py`) — clase `AverageWorker` / `SchedulerWorker`, contiene toda la lógica de negocio.
- **Redis** (OML y Dialer) — pub/sub, contadores, caché de prioridades, jobstore del scheduler.
- **PostgreSQL** (OML y Dialer) — persistencia de campañas, contactos, reglas de incidencia.
- **APScheduler** con backend Redis — agenda de contactos y relanzamiento de campañas fuera de horario.

La arquitectura reciente (rama `oml-773-dev-oml-3`) desacopló el dialer de Asterisk/ARI: el origina llamadas delega en un job Gearman `acd-call-processor` consumido por el ACD externo. El dialer queda enfocado en campañas, selección de contactos, contadores, reportes y reglas de incidencia.

El código tiene buena cobertura de los flujos principales y mecanismos defensivos bien pensados (reservas atómicas, dedup de decrementos, audit job). Las mejoras listadas a continuación van de deuda técnica menor hasta bugs funcionales bloqueantes.

---

## 2. Listado de Mejoras (de menor a mayor prioridad)

---

### 2.1 Prioridad Baja — Calidad de código y deuda técnica

#### B1 · Typo en respuesta de `create_incidence_rule`

**Archivo:** `workers/handle-campaign/src/handler/naive.py`, línea ~2761  
**Problema:** El worker devuelve `b'Incide rule was added'` (falta "nce").  
**Corrección:** Cambiar a `b'Incidence rule was added'`.

---

#### B2 · Mezcla de estilos de logging (f-strings vs `%s`)

**Archivo:** `naive.py` en general  
**Problema:** La mayoría de los `logger.debug/info/warning` usan f-strings, lo que fuerza la evaluación de la interpolación incluso cuando el nivel de log no lo requiere. Algunos usan `%s` correctamente.  
**Corrección:** Estandarizar a `logger.debug('mensaje %s', variable)` en todo el módulo para evitar overhead innecesario.

---

#### B3 · Mezcla de estilos de formateo de strings (`format()` vs f-strings)

**Archivo:** `naive.py`, métodos `take_contacts`, `process_campaign_inside`  
**Problema:** Quedan llamadas a `"Campaign {0}: contacts_attempts_number={1}".format(...)` mezcladas con f-strings modernos. Es inconsistente e innecesario.  
**Corrección:** Homogeneizar a f-strings o al estilo de logging con `%s`.

---

#### B4 · `TODO` pendientes documentados sin resolución

**Archivo:** `naive.py` y `app.py`  
Hay varios `# TODO` que reflejan decisiones postergadas:

- Línea 806: optimización de horario — pausar campaña y agendar reanudación en vez de loopear indefinidamente contra la BD.
- Línea 1333: frecuencia de la notificación `ALMOST_NO_CONTACTS` no definida.
- Línea 1821: `attempt_contact` podría ser background job (afecta throughput; ver A3).
- Línea 2436: limpieza de Redis en `delete_campaign` (ver M3).
- `app.py` línea 268: el endpoint HTMX `/` y el render-template deberían moverse al worker.

**Acción recomendada:** Triagear y convertir en issues rastreables; los que tienen impacto funcional están descritos con mayor detalle más adelante.

---

#### B5 · `timed_lru_cache` no es thread-safe

**Archivo:** `workers/handle-campaign/src/handler/utils.py`  
**Problema:** La variable `func.expiration` se lee y escribe sin lock. Si el scheduler APScheduler (que corre en un thread de background) y el thread principal del worker invocan la misma función cacheada simultáneamente, puede haber una race condition en la comparación y escritura de `expiration`.  
**Corrección:** Usar `threading.Lock` alrededor de la comparación y reset de `expiration`, o migrar a `cachetools.TTLCache` que es thread-safe.

---

#### B6 · Pool de conexiones Postgres demasiado pequeño

**Archivo:** `naive.py`, líneas 382 y 389  
**Problema:** Ambos pools usan `min_size=1, max_size=2`. Con múltiples jobs Gearman ejecutándose en paralelo en el mismo proceso worker (ej. `process-contact` + `send-reports` + `schedule-agenda` concurrentes), dos conexiones pueden no ser suficientes, generando timeouts de pool bajo carga.  
**Corrección:** Aumentar `max_size` a al menos 5–10, ajustando según el número de workers y la capacidad del servidor Postgres.

---

#### B7 · `ConnectionPool` con `open=True` crea conexión en tiempo de import

**Archivo:** `naive.py`, métodos `get_oml_connection` y `get_dialer_connection`  
**Problema:** El pool se crea con `open=True` la primera vez que se llama, lo que inicia la conexión inmediatamente. Si Postgres no está disponible al arrancar el worker (orden de inicio en Docker Compose), el proceso falla en el primer job en vez de reintentar.  
**Corrección:** Usar `open=False` y dejar que el pool gestione la apertura lazy, o agregar health check en el entrypoint.

---

#### B8 · CI/CD: lint y test deshabilitados en el pipeline

**Archivo:** `.gitlab-ci.yml`  
**Problema:** Los stages `lint` y `test` están comentados. No hay validación automática de calidad de código ni ejecución de tests en PRs/merges.  
**Corrección:** Reactivar los stages. El test suite existe (`tests.py` con 37 tests) y es funcional contra el compose de tests. El linter está configurado (`.flake8`).

---

### 2.2 Prioridad Media — Correctitud y confiabilidad

#### M1 · Sin autenticación en la API Flask

**Archivo:** `interface/src/app.py`  
**Problema:** Ningún endpoint tiene autenticación (API key, token Bearer, IP whitelist, etc.). Cualquier cliente con acceso de red puede crear, borrar, iniciar o pausar campañas, disparar llamadas manuales o cambiar la base de datos de contactos.  
**Corrección:** Implementar al menos un middleware de API key (header `X-API-Key`) con valor configurable via variable de entorno. Para mayor seguridad, restringir a red interna con firewall.

---

#### M2 · Redis sin autenticación configurada

**Archivo:** `naive.py` (conexiones Redis OML y Dialer) y `SchedulerWorker.JOBSTORES`  
**Problema:** Las conexiones a Redis no pasan `password`. En `JOBSTORES` hay un comentario explícito: `# password=os.getenv('REDIS_DIALER_PASSWORD')`. En entornos sin red privada segura, Redis queda expuesto.  
**Corrección:** Leer la contraseña de variables de entorno y pasarla a `redis.Redis()` y `RedisJobStore()`.

---

#### M3 · `delete_campaign` no limpia datos residuales en Redis

**Archivo:** `naive.py`, método `delete_campaign`, línea ~2436  
**Problema:** Al borrar una campaña, quedan en Redis: `CAMP:{id}:COUNTER`, `CAMP:{id}:COUNTER_PREV`, `CAMP:{id}:DISTRIBUTION`, `CAMP:{id}:SCHED`, `CAMP:{id}:SCHED:SEEN`, `OML:CALLS:{id}:DIALER`, `OML:CALLS:RESERVE_TS:{id}:*`, `OML:CALLS:DECR:{id}:*`, y todas las claves `CONTACT:*:CAMP:{id}` y `CONTACT:*:CAMP:{id}:HISTORY`. En despliegues con alta rotación de campañas esto es un memory leak real.  
**Corrección:** Agregar en `delete_campaign` un bloque que use `SCAN` + `DEL` sobre todos los patrones de claves de la campaña, similar a lo que ya hace `change_database` para las claves de contactos.

---

#### M4 · `opening_hours_match` hace 3–5 queries SQL en cada ciclo del loop principal

**Archivo:** `naive.py`, métodos `is_allowed_to_call` → `opening_hours_match`  
**Problema:** Se llama en cada iteración de `process_campaign_inside`. Para campañas con ciclos rápidos, esto genera docenas de queries por segundo contra la BD del dialer, consultando datos que cambian como máximo una vez por día.  
**Corrección:** Cachear el resultado con TTL de 30–60 segundos usando `timed_lru_cache` o almacenando en Redis (similar a como se cachea `CUSTOMDIALERDST`).

---

#### M5 · Falta índice compuesto `(id_campaign, status)` en `contact_in_campaign`

**Archivo:** `omnidialer.sql`  
**Problema:** Las queries críticas del loop principal filtran por `id_campaign` y `status` simultáneamente:
- `take_contacts`: `WHERE id_campaign = %s AND (status = %s OR schedule_aborted = true) LIMIT %s`
- `campaign_is_active`: `WHERE status = %s AND id_campaign = %s GROUP BY status`
- `send_reports`: `WHERE id_campaign = %s AND (status = %s OR status = %s)`

Solo existen índices separados sobre `id_campaign` y sobre `id_contact`. Para campañas con decenas de miles de contactos, el planner de Postgres puede no usar el índice de `id_campaign` eficientemente con el filtro adicional de `status`.  
**Corrección:**
```sql
CREATE INDEX idx_cic_campaign_status ON contact_in_campaign (id_campaign, status);
CREATE INDEX idx_cic_campaign_final_status ON contact_in_campaign (id_campaign, final_status);
```

---

#### M6 · `PROCESS-CAMPAIGN-{id}` lock Redis sin TTL — riesgo de campaña bloqueada

**Archivo:** `naive.py`, método `check_running_job`  
**Problema:** La clave `PROCESS-CAMPAIGN-{id}` se crea con `SET ... NX` sin TTL. Si el proceso worker es terminado abruptamente (SIGKILL, OOM killer, crash), la clave no se limpia en el `finally` y la campaña queda bloqueada indefinidamente. Existen `remove-locks.py` y `redis-restore.py` como remedios manuales, pero no hay recuperación automática.  
**Corrección:** Agregar un TTL razonable al SET (ej. 30 minutos, mayor al tiempo máximo esperado de un ciclo de campaña):
```python
cls.REDIS_DIALER_CONNECTION.set(
    f'PROCESS-CAMPAIGN-{id_campaign}', 'True', nx=True, ex=1800
)
```
O implementar un watchdog que renueve el TTL mientras el ciclo esté activo.

---

#### M7 · `campaign_is_active` mide porcentaje de contactos con query sin índice optimizado

**Archivo:** `naive.py`, líneas ~1334–1356  
**Problema:** La query que calcula el porcentaje de contactos llamados hace un `COUNT(*) * 100.0 / (SELECT COUNT(*) FROM contact_in_campaign WHERE id_campaign = %s)` en cada ciclo del loop. Para campañas grandes, es una query de escaneo completo sobre todos los contactos de la campaña. Se ejecuta en cada iteración aunque el resultado no cambia hasta el siguiente evento de llamada.  
**Corrección:** Mover este cálculo al job `send_reports` que ya tiene toda la información de conteos, y comunicar el resultado vía Redis (como ya hace con `PENDING_INITIAL_CONTACT_ATTEMPTS`). En `campaign_is_active`, leer el valor desde Redis.

---

### 2.3 Prioridad Alta — Bugs funcionales / Features incompletas

#### A1 · `attempt_contact` es síncrono: bloquea el loop de campañas

**Archivo:** `naive.py`, método `attempt_contact`, línea ~1822  
**Problema:** `cls.GM_CLIENT.submit_job('process-contact', message)` sin `background=True` espera a que el worker procese el job antes de continuar. Esto serializa completamente el loop de `process_campaign_inside`: no se puede iniciar la siguiente llamada hasta que la anterior complete `process-contact`. El propio código lo marca como `# TODO: think if the following could be a background job call`.

Con el diseño actual, el throughput del dialer está limitado a ~1 llamada iniciada por tiempo de latencia de Gearman + `process-contact`. Para campañas con `max_channels` alto o `boost_factor > 1`, esto es un cuello de botella severo.  
**Corrección:** Cambiar a `background=True`:
```python
cls.GM_CLIENT.submit_job('process-contact', message, background=True)
```
La gestión de errores se hace por la reserva atómica y el audit job ya existentes.

---

#### A2 · `audit_active_channels` siempre falla en producción

**Archivo:** `naive.py`, método `_fetch_asterisk_dialer_channel_counts`  
**Problema:** El audit job (que corre cada 60s vía APScheduler) llama a `submit_job('audit-dialer-channels', b'{}')` esperando que el ACD responda con el conteo de canales activos. Sin embargo, no existe ningún componente registrado como worker Gearman para el job `audit-dialer-channels` en este repositorio ni en ningún lugar documentado.

El resultado es que `_fetch_asterisk_dialer_channel_counts` siempre lanza una excepción (timeout o job fallido), `audit_active_channels` la captura y loggea un error crítico cada 60 segundos, y la reconciliación de contadores nunca ocurre. Los contadores `OML:CALLS:{id}:DIALER` pueden quedar inflados indefinidamente si se pierde algún evento terminal.  
**Corrección:** Implementar el worker `audit-dialer-channels` en el ACD externo (que tiene acceso a los canales Asterisk), o documentar claramente que este mecanismo es un stub pendiente de completar y deshabilitarlo con `DIALER_CHANNEL_AUDIT_INTERVAL_SEC=0` en producción hasta que esté listo.

---

#### A3 · Workers para llamadas manuales no implementados

**Archivos:** `interface/src/dialer/multichannel.py`, `workers/handle-campaign/src/app.py`  
**Problema:** La API Flask expone y encola los siguientes jobs Gearman:
- `manual-call` (endpoint `/manual-call/<id_campaign>`)
- `external-manual-call` (endpoint `/external-manual-call`)
- `agent2agent-call` (endpoint `/agent2agent`)
- `call-campaign-contact` (endpoint `/call-campaign-contact/<id_campaign>`)

Ninguno de estos jobs está registrado en el `JOBS_TO_METHODS` del worker, ni implementado en `AverageWorker`. La API devuelve `202 Accepted` pero los jobs se acumulan en Gearman sin ser procesados.

Esto también se confirma en `status.md`: *"Llegue hasta crear un nuevo endpoint para generar llamadas manuales. Eso implica un nuevo worker manual-call."*  
**Corrección:** Implementar los handlers correspondientes en `AverageWorker` y registrarlos en `app.py`. Para llamadas manuales/preview, el flujo sugerido es: validar agente disponible → aplicar prefijo → llamar `trigger_acd_dial` → retornar resultado.

---

## 3. Tabla Resumen

| ID | Área | Descripción breve | Prioridad |
|----|------|--------------------|-----------|
| B1 | Código | Typo en respuesta `create_incidence_rule` | Baja |
| B2 | Código | f-strings en logging (overhead) | Baja |
| B3 | Código | Mezcla `format()` / f-strings | Baja |
| B4 | Deuda | TODOs sin resolver | Baja |
| B5 | Concurrencia | `timed_lru_cache` no thread-safe | Baja |
| B6 | Infraestructura | Pool Postgres demasiado pequeño (max=2) | Baja |
| B7 | Infraestructura | `open=True` en ConnectionPool | Baja |
| B8 | CI/CD | Lint y tests deshabilitados en pipeline | Baja |
| M1 | Seguridad | Sin autenticación en API Flask | Media |
| M2 | Seguridad | Redis sin contraseña configurada | Media |
| M3 | Memoria | `delete_campaign` no limpia Redis | Media |
| M4 | Performance | `opening_hours_match` sin caché en loop | Media |
| M5 | Performance | Falta índice compuesto `(id_campaign, status)` | Media |
| M6 | Confiabilidad | Lock `PROCESS-CAMPAIGN-{id}` sin TTL | Media |
| M7 | Performance | Query de porcentaje en `campaign_is_active` en cada ciclo | Media |
| A1 | Throughput | `attempt_contact` síncrono bloquea loop | Alta |
| A2 | Confiabilidad | `audit_active_channels` siempre falla (job sin worker) | Alta |
| A3 | Funcional | Workers de llamadas manuales no implementados | Alta |

---

## 4. Notas sobre la Arquitectura Actual

El desacople de Asterisk/ARI es una dirección correcta. Los mecanismos de reserva atómica, dedup de decrementos, distribución por prioridad y el scheduler idempotente son buenos patrones. Los issues de alta prioridad son principalmente **features incompletas** (A3) y un **mecanismo de auditoría que no tiene su contraparte en el ACD** (A2), más que problemas de diseño fundamental.

Una vez implementados los workers de llamadas manuales (A3) y el consumer de `audit-dialer-channels` en el ACD (A2), y aplicadas las correcciones de throughput (A1) e índices (M5), el sistema debería comportarse de forma robusta y escalable horizontalmente agregando más procesos worker Gearman.
