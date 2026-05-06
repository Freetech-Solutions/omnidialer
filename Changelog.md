# Changelog - Dialer OML 3.0

## Alcance

Comparacion tecnica entre `main` y `oml-773-dev-oml-3` para el componente:

`components-git-repo/dialer`

La rama concentra los avances de evolucion del dialer hacia una arquitectura mas simple y desacoplada de Asterisk. El analisis ignora cambios menores de formato y se enfoca en comportamiento, arquitectura, integracion e impacto operativo.

## Resumen Ejecutivo

La rama `oml-773-dev-oml-3` simplifica el dialer eliminando la responsabilidad directa de administrar Asterisk, ARI, dialplan y websocket ARI dentro de este repositorio. El worker deja de originar llamadas mediante `ARI.originate` y pasa a delegar el discado al ACD mediante Gearman, usando el job `acd-call-processor`. El valor principal es el desacople operativo: el dialer queda enfocado en campanas, seleccion de contactos, agenda, reglas, contadores, reportes y procesamiento de eventos normalizados.

## Nuevas Funcionalidades

- Discado desacoplado via ACD. El worker ahora construye un payload con `command`, `number`, `campaign_id`, `contact_id`, `agent_id` y `metadata`, y lo encola en Gearman para que el ACD origine la llamada.
- Soporte de blacklist desde Redis. Antes de llamar, el worker consulta `OML:BLACKLIST`; si el numero esta bloqueado, no se dispara la llamada, se libera la reserva y el contacto queda marcado como no contactable.
- Nuevos estados de resultado de llamada: `INVALID_NUMBER`, `AMD`, `EXIT_SHORTCALL` y `CANCEL`, con impacto en historial, metricas y reglas de incidencia.
- Mejor soporte para AMD. El evento AMD deja de mapearse genericamente como `TERMINATED` y pasa a tener entidad propia para estadisticas y reglas.
- Agenda mas robusta. El scheduler incorpora control idempotente de eventos `ADDED`, `EXECUTED`, `ERROR`, `REMOVED` y `MISSED`, usando `seen set`, TTL y deteccion de reemplazos.
- Procesamiento de eventos mas estricto. Los eventos telefonicos ahora deben traer campos explicitos de negocio: `id_campaign`, `contact_id` y `phone_number`.
- Mejor sincronizacion de llamadas activas. El contador `OML:CALLS:{id_campaign}:DIALER` se reserva antes de encolar la llamada y se libera con eventos terminales.

## Cambios Arquitectonicos / Tecnicos

### Desacople con Asterisk

Antes, el dialer tenia una dependencia directa con Asterisk/ARI:

- El worker importaba `ari_manager.py`.
- Se configuraban variables `ASTERISK_USER`, `ASTERISK_PASS`, `ASTERISK_HOST`, `ASTERISK_PORT` y `ASTERISK_APP`.
- El metodo de discado armaba endpoint PJSIP, caller id, headers SIP, `appArgs` y llamaba directamente a `ARI.originate_channel`.
- El repositorio incluia imagenes y configuraciones propias para `asterisk/`, `dialplan/` y `websocket_ari/`.

Ahora, el worker ya no administra ARI ni origina canales. La funcion central de discado es `trigger_acd_dial`, que solo publica un job Gearman `acd-call-processor`. La responsabilidad de abrir canales, validar rutas, aplicar dialplan y resolver troncales queda fuera del dialer, en el ACD.

### Componentes eliminados del repositorio

Se eliminan del scope local:

- `asterisk/`
- `dialplan/`
- `websocket_ari/`
- `workers/handle-campaign/src/handler/ari_manager.py`
- `docker-compose.yml` monolitico con servicios de Asterisk/dialplan/listener

Tambien se remueven del pipeline las imagenes:

- `dialer_asterisk`
- `dialer_dialplan`
- `dialer_listener`

La CI queda enfocada en:

- `dialer_api`
- `dialer_worker`

### Infraestructura resultante

El dialer queda compuesto principalmente por:

- API Flask (`interface/src/app.py`)
- Workers Gearman (`workers/handle-campaign`)
- Redis de OML y Redis del dialer
- Postgres OML y Postgres del dialer
- Gearman como bus de trabajos
- ACD externo para originacion y eventos telefonicos

Redis gana peso como fuente operativa para:

- campanas activas: `campaigns:active`
- distribucion por prioridad: `CAMP:{id}:DISTRIBUTION`
- llamadas activas: `OML:CALLS:{id}:DIALER`
- agendas: `CAMP:{id}:SCHED`
- historial de contacto: `CONTACT:{contact_id}:CAMP:{campaign_id}:HISTORY`
- blacklist: `OML:BLACKLIST`
- pub/sub hacia `OML:CHANNEL:DIALER`

## Como funciona ahora el listener

Hay dos conceptos de listener.

### Listener telefonico / ACD

El listener ARI ya no vive dentro de este repositorio. La expectativa nueva es que un componente externo, asociado al ACD, escuche los eventos telefonicos, los normalice y los envie a Gearman como job `process-event`.

El contrato esperado por el worker ahora es explicito:

```json
{
  "id_campaign": "4",
  "contact_id": "123",
  "phone_number": "5491112345678",
  "type": "Dial",
  "call_type": "to_pstn",
  "dialstatus": "ANSWER",
  "callid": "..."
}
```

Esto reemplaza la inferencia anterior basada en `peer.caller.name`. Si faltan `id_campaign`, `contact_id` o `phone_number`, el worker falla rapido para evitar reportes inconsistentes.

Eventos relevantes:

- `Dial` con `ANSWER`: actualiza `ANSWERED_PSTN` o `ANSWERED_AGENT`.
- `Dial` con fallos: procesa `BUSY`, `NOANSWER`, `CONGESTION`, `TIMEOUT`, `CHANUNAVAIL`, `INVALID_NUMBER`, `CANCEL`, `AMD` y `EXIT_SHORTCALL`.
- `ChannelDestroyed` / `ChannelDestroy`: decrementa llamadas activas cuando libera canal PSTN.
- `RouteValidationFailed`: libera contador y reporta cuando el ACD bloquea una llamada por validacion de ruta.

### Listener del scheduler

El listener de agenda vive en `SchedulerWorker` y escucha eventos internos de APScheduler:

- `ADDED`
- `EXECUTED`
- `ERROR`
- `REMOVED`
- `MISSED`

Su responsabilidad es mantener el contador de agendas (`CAMP:{id}:SCHED`) de forma confiable:

- incrementa solo si el job no existia;
- decrementa una sola vez por `job_id`;
- evita decrementos falsos cuando un job se reemplaza con `replace_existing=True`;
- usa `SCHED:REPLACING:{job_id}` con TTL corto para distinguir reemplazos reales;
- usa `CAMP:{id}:SCHED:SEEN` para idempotencia.

## Como funciona ahora el discado de nuevas llamadas

1. La API recibe la accion de iniciar o reanudar campana y encola `start-campaign` o `resume-campaign`.
2. El worker marca la campana como activa, limpia contactos seleccionados previamente y encola `process-campaign`.
3. `process-campaign` valida que el dialer y la campana esten activos.
4. Se valida horario de llamada. Si no corresponde llamar, se agenda un `process-campaign` futuro.
5. Se calcula cuantas llamadas nuevas se pueden lanzar usando:
   - agentes disponibles;
   - `boost_factor`;
   - `max_channels`;
   - llamadas activas actuales;
   - prioridad relativa entre campanas activas;
   - modo predictivo o power dialer.
6. Se seleccionan contactos pendientes y se marcan como `STATUS_SELECTED_CALL`.
7. Antes de encolar la llamada, el worker reserva capacidad incrementando `OML:CALLS:{id_campaign}:DIALER`.
8. Si la reserva supera `max_channels`, se revierte el contador y el contacto vuelve a `STATUS_CREATED`.
9. Si la reserva es valida, se encola `process-contact`.
10. `process-contact` valida blacklist, estado de campana y horario.
11. Aplica prefijo si existe.
12. Encola `acd-call-processor` con el numero final.
13. Si falla el enqueue hacia Gearman, revierte la reserva y devuelve el contacto a `STATUS_CREATED`.
14. Cuando llegan eventos terminales desde el ACD, `process-event` actualiza estados, reportes, reglas de incidencia y contadores.

## Modos de discado

### Predictivo

Se usa cuando no hay `CUSTOMDIALERDST` y `VOICEBOT` no esta activo. La cantidad de nuevas llamadas se calcula como:

```text
target = agentes_disponibles_equivalentes * boost_factor
nuevas_llamadas = target - llamadas_activas
```

El resultado queda limitado por `max_channels` y por la distribucion de prioridad entre campanas activas.

### Power Dialer / Voicebot

Se usa cuando:

- `CUSTOMDIALERDST != '0'`, o
- `VOICEBOT=True`

En este modo, el worker intenta llenar la capacidad disponible hasta `max_channels`, sin depender del calculo predictivo por agentes.

## Procesamiento de eventos y reportes

El procesamiento de eventos se vuelve mas deterministico:

- Los eventos intermedios como `Dial` sin `dialstatus` o con `RINGING` se ignoran para no contaminar metricas.
- `ANSWERED_PSTN` marca respuesta de troncal.
- `ANSWERED_AGENT` marca contacto exitoso y finaliza el contacto como `FINALIZED_SUCCESS`.
- Fallos aplican reglas de incidencia cuando corresponde.
- `CANCEL`, `AMD` y `EXIT_SHORTCALL` decrementan llamadas activas directamente, porque no siempre llega un `ChannelDestroyed` equivalente.
- Campana/contacto `0` se consideran eventos de sistema y no generan actualizaciones de estado ni reportes de base.

## Cambios en API

La API Flask agrega respuestas JSON y codigos HTTP explicitos para nuevas llamadas encoladas:

- `202 Accepted` para llamadas manuales/preview aceptadas en cola.
- `400 Bad Request` cuando faltan parametros requeridos.

Tambien se normaliza `prefix` en creacion de campana:

- `[]` se convierte a `None`;
- listas con valores se convierten al primer valor como string;
- valores escalares se guardan como string.

## Cambios en datos y persistencia

No se detectan cambios de esquema en `omnidialer.sql` entre `main` y `oml-773-dev-oml-3`. La columna `campaign.prefix` ya existia en la rama base.

Los cambios de persistencia son principalmente de uso:

- cache de `VOICEBOT` desde Redis OML hacia Redis Dialer;
- cache de `CUSTOMDIALERDST`;
- uso mas estricto de `OML:CALLS:{id_campaign}:DIALER`;
- historial de contacto en Redis para reglas de incidencia;
- uso de `CAMP:{id}:SCHED:SEEN` para idempotencia de agenda.

## Cambios en CI/CD e infraestructura

El pipeline deja de construir artefactos relacionados con Asterisk local:

- `container-image-asterisk`
- `container-image-dialplan`
- `container-image-websocket_ari`

Quedan builds principales:

- `container-image-api`
- `container-image-worker`

Tambien se eliminan variables `ASTERISK_*` de archivos de testing y compose simplificados, reforzando que el worker ya no necesita credenciales ARI.

## Impacto para QA

Validar:

- inicio, pausa, reanudacion y finalizacion de campanas;
- calculo de llamadas permitidas en modo predictivo;
- comportamiento power dialer con `CUSTOMDIALERDST` y `VOICEBOT=True`;
- que no se supere `max_channels` bajo concurrencia;
- rollback de contador cuando falla el enqueue hacia ACD;
- decremento por `ChannelDestroyed`, `RouteValidationFailed`, `CANCEL`, `AMD` y `EXIT_SHORTCALL`;
- aplicacion de reglas de incidencia para fallos y AMD;
- bloqueo por blacklist;
- agenda/reagenda sin doble conteo;
- reportes y pub/sub hacia `OML:CHANNEL:DIALER`;
- contrato de eventos normalizados desde el ACD.

## Impacto para DevOps

El despliegue debe considerar que el dialer ya no trae Asterisk/dialplan/listener en este repositorio. Para un entorno funcional se requiere:

- Gearman disponible para API, workers y ACD;
- un consumidor externo del job `acd-call-processor`;
- un listener externo que publique eventos normalizados hacia `process-event`;
- Redis OML y Redis Dialer correctamente configurados;
- Postgres OML y Postgres Dialer accesibles;
- workers registrados para los jobs necesarios mediante `GEARMAN_JOBS`.

## Pendientes / Riesgos Detectados

- La API encola `manual-call`, `call-campaign-contact`, `external-manual-call` y `agent2agent-call`, pero esos jobs no aparecen registrados en `workers/handle-campaign/src/app.py` ni implementados en `AverageWorker` en esta rama. Esto debe completarse antes de QA end-to-end de llamadas manuales/preview/agent-to-agent.
- El metodo `audit_active_channels` queda preparado conceptualmente para reconciliar canales, pero actualmente no consulta una fuente real de canales del ACD.
- El contrato exacto del `acd-call-processor` queda fuera de este repositorio; debe documentarse junto al componente ACD para cerrar la integracion.

## Resultado

La rama reduce el alcance del dialer y mejora su mantenibilidad: el repositorio deja de cargar con telefonia de bajo nivel y pasa a operar como orquestador de campanas y jobs. La simplificacion permite evolucionar el ACD, el listener y el dialer de forma independiente, siempre que se mantenga estable el contrato Gearman de discado y eventos.
