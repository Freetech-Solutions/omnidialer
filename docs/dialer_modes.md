# Modos de discado de OMniDialer y algoritmo de predictividad

Documento de referencia del motor de pacing implementado en
`workers/handle-campaign/src/handler/naive.py` (clase `AverageWorker`) y
`workers/handle-campaign/src/handler/predictive_pacer.py` (helpers puros).

**Alcance:** cómo se decide el modo de discado de una campaña, cómo calcula el
modo predictivo la cantidad de llamadas a originar en cada ciclo (`C_dial`), y
todos los parámetros de configuración que lo gobiernan.

---

## 1. Modos de discado

En cada ciclo de `process_campaign_inside`, el worker llama a
`allowed_parallel_contact_attempts(id_campaign)`, que primero resuelve el modo
con `resolve_dial_mode()` (orden de prioridad):

| Prioridad | Modo | Condición | Comportamiento |
|-----------|------|-----------|----------------|
| 1 | `power` | `CAMP:{id}:CUSTOMDIALERDST != '0'` o `CAMP:{id}:VOICEBOT=true` en Redis | Llena hasta `max_channels` (todos los canales libres). Pensado para voicebots / destinos custom sin agentes |
| 2 | `predictive` | `campaign.initial_predictive_model = true` (Postgres dialer) **y** `DIALER_PREDICTIVE_ENABLED=true` | Fórmula predictiva `C_dial` (ver §3) |
| 3 | `progressive` | Resto de los casos (incluye flag de campaña en true pero kill-switch global en false) | `target = A_free × initial_boost_factor`; nuevas = `target − (RINGING + WAITING_AGENT)` (ONCALL no resta) |

Notas:

- `initial_boost_factor` e `initial_predictive_model` se leen de la tabla
  `campaign` de la base `omnidialer` con cache de 600 s
  (`get_boost_factor` / `get_predictive_model`).
- El progresivo con `initial_boost_factor = 1.0` es el "progresivo estricto"
  (R=1): una originación por agente READY **sin** contar canales ONCALL
  (esos ya están con agentes ocupados). Es el modo de seguridad al que cae
  el predictivo en warm-up, sin muestra o con kill-switch latcheado.
- El resultado del modo siempre queda limitado por el headroom de canales:
  `num_available_channels = max_channels − OML:CALLS:{id}:DIALER` (sí incluye
  ONCALL), y luego por la prorrateo de prioridad entre campañas
  (`allowed_calls_prority_percentage`) y el rate limit `DIALER_CAPS`
  (originaciones por segundo) aplicado dentro del loop.

---

## 2. Ciclo de evaluación (tick loop)

```text
process_campaign_inside(id_campaign)          # loop mientras la campaña esté ACTIVE
  └─ is_allowed_to_call()                     # agenda / días / horarios
  └─ allowed_parallel_contact_attempts()      # modo → C_dial del ciclo
  └─ allowed_calls_prority_percentage()       # prorrateo por PRIORITY entre campañas
  └─ take_contacts(N)                         # marca contactos STATUS_SELECTED_CALL
       └─ por contacto: _reserve_dialer_channel()  # INCR OML:CALLS + fase RINGING (atómico Lua)
          └─ attempt_contact() → Gearman 'process-contact'
          # rate limit: máximo DIALER_CAPS originaciones por segundo
```

Cadencia cuando no hay contactos seleccionados en el ciclo:

- Modo `predictive` con `DIALER_PREDICTIVE_TICK_MS > 0` →
  `sleep(PREDICTIVE_TICK_MS / 1000)` (default 1 s). Es el "tick" del modelo.
- Resto → `sleep(DIALER_TIME_BETWEEN_CALLS)` (default 3 s).

---

## 3. Algoritmo predictivo

### 3.1 Fórmula operativa

En cada tick, `_allowed_parallel_predictive()` calcula:

```text
C_dial = max(0, floor( (A_free + A_expected − C_ringing × P_hit) / P_hit × γ ))
```

y luego aplica el cap de canales: `min(C_dial, max_channels − canales_activos)`.

Interpretación: se dimensiona el déficit de agentes
(`A_free + A_expected − connects esperados de lo que ya está sonando`) y se
divide por `P_hit` para **sobre-marcar** compensando los intentos que no
conectan con humano. `γ` modula la agresividad según el margen al abandono
máximo permitido.

### 3.2 Variables de entrada

| Variable | Definición | Fuente |
|----------|------------|--------|
| `A_free` | Agentes en `READY`, ponderados multi-cola (`weight = 1/n_colas`); una fracción > 0 cuenta como 1 (semántica legacy) | `OML:AGENT:{id}` (Redis OML DB0): `STATUS`, `TIMESTAMP` |
| `A_expected` | Esperanza de agentes ocupados que se liberan durante el horizonte `t_ring`: `Σ P_lib,i × w_i` sobre `ONCALL` / `POSTCALL` / `PAUSE-ACW` | Snapshot de agentes + medias ATT/ACW/AHT |
| `C_ringing` | Llamadas dialer en fase `RINGING` (originadas sin answer humano útil todavía) | `HGET CAMP:{id}:CHANNELS RINGING` (Redis dialer DB3) |
| `P_hit` | Probabilidad de connect humano (EWMA), con piso `HIT_RATE_FLOOR` | `CAMP:{id}:METRICS` → `P_HIT` |
| `γ` (gamma) | Factor de corrección por margen al `D_max` | Calculado por `compute_gamma()` |
| `aggressiveness` | Techo de γ en zona sana = `initial_boost_factor` de la campaña (clamp `[0.1, 5.0]`) | Postgres dialer `campaign.initial_boost_factor` |

### 3.3 Horizonte de predicción `t_ring`

```text
t_ring = ART + amd_extra
```

- `ART` (Average Ring Time): media aritmética de `ring_duration` reportada por
  el ACD en el evento `ANSWERED_PSTN`, acumulada en `CAMP:{id}:ART`
  (`ART_SUM`, `ART_COUNT`, `ART`). Sin muestra → `DIALER_DEFAULT_ART_SEC`
  (default 15 s).
- `amd_extra` (H7):
  - `0` si la campaña no tiene AMD (`OML:CAMP:{id}.AMD` falso).
  - Media medida en `CAMP:{id}:AMD_LATENCY` (`AMD` / `METRICS.AMD_TIME`) cuando
    `AMD_COUNT > 0` (eventos Gearman `AmdLatency` desde ACD al salir de `[amd]`).
  - Sin muestra: `TOTAL_ANALYSIS_TIME` de `OML:AMD_CONF:1` (ms→s), con fallback
    `DIALER_DEFAULT_AMD_FALLBACK_SEC` (default 5 s).

### 3.4 Probabilidad de liberación `P_lib` y `A_expected`

Para cada agente ocupado "liberable" (`ONCALL`, `POSTCALL`, `PAUSE-ACW`):

```text
remaining = max(P_LIB_REMAINING_EPS, μ − elapsed)
P_lib     = 1 − exp(−t_ring / remaining)      # exponencial de supervivencia
```

- `elapsed = now − OML:AGENT:{id} TIMESTAMP` (entrada al estado actual).
- `μ` = media de ocupación residual según estado:
  - `ONCALL` → `AHT` (o `ATT + ACW` si el hash AHT aún no existe)
  - `POSTCALL` / `PAUSE-ACW` → `ACW` (o `AHT − ATT` como fallback)
- `P_LIB_REMAINING_EPS` (default 1 s) evita división por cero cuando el agente
  ya superó la media.
- Agentes en otros estados (p. ej. `PAUSE` genérica) no aportan: `P_lib = 0`.
- `A_expected = Σ P_lib,i × weight_i` con el mismo peso multi-cola
  `1/n_colas` que `A_free`.

### 3.5 Estimación de `P_hit` y taxonomía de eventos

Los eventos Dial que llegan vía Gearman `process-event` actualizan
`CAMP:{id}:METRICS` con una Lua atómica (`_UPDATE_CAMPAIGN_HIT_LUA`):

| Evento | Clasificación | Efecto en métricas |
|--------|---------------|--------------------|
| `ANSWERED_PSTN` | **Hit** (connect humano pre-agente) | `HIT_COUNT++`, `CONNECT_COUNT++`, `P_HIT ← EWMA(1)`, `DROP_RATE_EWMA ← EWMA(0)` |
| `BUSY`, `NOANSWER`, `CONGESTION`, `TIMEOUT`, `TERMINATED`, `CHANUNAVAIL`, `INVALID_NUMBER`, `CANCEL`, `AMD`, `EXIT_SHORTCALL`, `ORIGINATE_FAILED`, `480_*`, `404_*` | **Fail** de contacto | `FAIL_COUNT++`, `P_HIT ← EWMA(0)`; **no** toca `DROP_RATE_EWMA` |
| `EXIT_ABANDON`, `EXIT_TIMEOUT` | **Abandon** (post-connect, sin agente) | `ABANDON_COUNT++`, `DROP_RATE_EWMA ← EWMA(1)`; **no** toca `P_HIT` ni `FAIL_COUNT` |
| `AmdLatency` | Solo métrica H7 | Actualiza `AMD_LATENCY` + `METRICS.AMD_TIME`; sin status/DECR |
| `ANSWERED_AGENT`, `EXIT_ANSWERED`, `EXIT_ACW`, `ChannelDestroyed`, … | Fuera de taxonomía hit/fail | No modifican METRICS de pacing |

Ratios persistidos en el mismo hash:

```text
P_HIT_RATIO = HIT_COUNT / (HIT_COUNT + FAIL_COUNT)     # ratio acumulado crudo
DROP_RATE   = ABANDON_COUNT / HIT_COUNT                # reporting acumulado
DROP_RATE_EWMA = EWMA simétrico (α = DROP_RATE_ALPHA)  # fuente canónica para γ
```

- `P_HIT` es un EWMA con `α = 0.1` (constante `P_HIT_ALPHA` en `naive.py`,
  no configurable por env).
- El pacing lee `P_HIT` (EWMA) y le aplica el piso `HIT_RATE_FLOOR`
  (anti-división por cero / anti-explosión de `C_dial` cuando el hit rate es
  muy bajo). Si todavía no hay muestra (`HIT_COUNT + FAIL_COUNT = 0`), `P_hit`
  es `None` y la campaña cae a fallback progresivo.
- `DROP_RATE_EWMA` es un EWMA **simétrico**: sube hacia 1 en cada abandono y
  decae hacia 0 en cada hit (fails no lo mueven). `get_campaign_drop_rate`
  lo usa como `D` para γ. Ventana efectiva ≈ `2/α − 1` connects (`α=0.1` ⇒
  ~19). `DROP_RATE` acumulado queda solo para reporting/logs. Hashes legacy
  sin el campo caen al ratio acumulado hasta el primer evento post-deploy
  (`WINDOW_MODE=ewma_symmetric`).
- Campañas con `DROP_RATE_EWMA` stale (p.ej. 0.9 de la era degenerada) se
  auto-curan: ×0.9 por hit → `< D_max` en ~33 hits (conservador: throttle de
  más). Opcional en deploy: `HDEL` de `DROP_RATE_EWMA` vía SCAN para arrancar
  limpio.

### 3.6 Drop rate y factor γ

`compute_gamma(drop_rate, D_max, aggressiveness, gamma_floor)`:

| Condición | γ |
|-----------|---|
| `drop_rate` es `None` (sin connects todavía) | se trata como 0 → `aggressiveness` |
| `D ≤ 0.5 × D_max` (zona sana) | `aggressiveness` (= `initial_boost_factor`) |
| `0.5 × D_max < D < D_max` (zona de freno) | interpolación lineal de `aggressiveness` → `GAMMA_THROTTLE_FLOOR` |
| `D ≥ D_max` | `0` en `compute_gamma`; el pacer aplica soft floor o kill-switch (ver §3.7) |

### 3.7 Máquina de estados del pacer

`decide_predictive_pace()` + `_update_throttle_streak()` (visible en logs como
`PREDICTIVE_WARMUP`, `PREDICTIVE`, `PREDICTIVE_THROTTLED`,
`PREDICTIVE_FALLBACK`):

| Modo | Condición | Acción |
|------|-----------|--------|
| `warmup` | `ATT_COUNT < DIALER_WARM_UP_SAMPLE_SIZE` | Progresivo R=1 (`target = A_free − RINGING − WAITING`) mientras se junta muestra |
| `progressive_fallback` | Sin muestra de `P_hit` (`HIT+FAIL = 0`) | Progresivo R=1 |
| soft hold | `D ≥ D_max` y streak `< K` (sin latch) | Predictive con `γ = GAMMA_THROTTLE_FLOOR` (`reason=drop_over_dmax_soft`) |
| `throttled` | Latch activo (`streak ≥ K` o latched y `D ≥ 0.8×D_max`) | Progresivo R=1; `EVENT=THROTTLE_ENGAGED` al engarzar |
| `predictive` | Resto | Fórmula completa `C_dial` con cap de canales |

Kill-switch (P3): `CAMP:{id}:THROTTLE_STREAK` se incrementa cada tick con
`D ≥ D_max`; al llegar a `DIALER_THROTTLE_STREAK_K` (default 5) se setea
`CAMP:{id}:THROTTLE_LATCH` + `WARNING` + evento en `CAMP:{id}:PACING`.
Salida con histéresis solo cuando `D < DIALER_THROTTLE_EXIT_RATIO × D_max`
(default 0.8) → `EVENT=THROTTLE_CLEARED`.

El warm-up / throttled reutiliza `_allowed_parallel_progressive`:
`calls_to_dial = ceil(A_free × boost) − (RINGING + WAITING_AGENT)`. ONCALL
no consume cupo de READY (un agente libre no queda bloqueado porque otro
esté en conversación). El headroom `max_channels − OML:CALLS` sigue
aplicando después.

El warm-up se mide sobre `ATT_COUNT` del hash `CAMP:{id}:ATT` (llamadas con
`EXIT_ANSWERED` que ya tuvieron conversación con agente), no sobre intentos.

### 3.8 Caps duros (siempre activos)

1. `max_channels` de la campaña: `C_dial ≤ max_channels − canales_activos`
   (`apply_channel_caps`).
2. Reserva atómica por contacto (`_RESERVE_CHANNEL_LUA`): si no hay cupo, el
   contacto vuelve a `STATUS_CREATED`.
3. `DIALER_CAPS`: máximo de originaciones por segundo dentro del loop.
4. Prorrateo por prioridad entre campañas (`allowed_calls_prority_percentage`).
5. Sin agentes READY ni esperados, `C_dial` tiende a 0 por la propia fórmula
   (en progresivo, `A_free = 0` ⇒ no disca).

---

## 4. Parámetros de configuración

### 4.1 Variables de entorno (globales, por despliegue)

Definidas en `workers/handle-campaign/src/settings/default.py`. Valores de
ejemplo tomados de `docker-compose/test-env/.env`:

| Env var | Setting | Default | Qué controla |
|---------|---------|---------|--------------|
| `DIALER_PREDICTIVE_ENABLED` | `PREDICTIVE_ENABLED` | `true` | Kill-switch global. En `false`, toda campaña con `initial_predictive_model=true` cae a progresivo (con boost) |
| `DIALER_MAX_ABANDON_RATE` | `MAX_ABANDON_RATE` | `0.03` | `D_max`: techo de abandono reciente (`DROP_RATE_EWMA`). Gobierna las zonas de γ y el modo throttled |
| `DIALER_WARM_UP_SAMPLE_SIZE` | `WARM_UP_SAMPLE_SIZE` | `50` | Cantidad de llamadas atendidas (`ATT_COUNT`) en progresivo estricto antes de habilitar el modo predictivo |
| `DIALER_PREDICTIVE_TICK_MS` | `PREDICTIVE_TICK_MS` | `1000` | Cadencia (ms) del ciclo de evaluación cuando una campaña predictiva no tiene contactos para discar en el ciclo. `0` desactiva el tick rápido y usa `DIALER_TIME_BETWEEN_CALLS` |
| `DIALER_HIT_RATE_FLOOR` | `HIT_RATE_FLOOR` | `0.05` | Piso de `P_hit` en el pacing: evita división por cero y explosión de `C_dial` con hit rates muy bajos |
| `DIALER_DROP_RATE_ALPHA` | `DROP_RATE_ALPHA` | `0.1` | α del EWMA simétrico `DROP_RATE_EWMA` (fuente de γ; ventana efectiva ≈ `2/α − 1` connects) |
| `DIALER_DEFAULT_ART_SEC` | `DEFAULT_ART_SEC` | `15` | ART de fallback (segundos) para `t_ring` mientras no hay muestra en `CAMP:{id}:ART` |
| `DIALER_DEFAULT_AMD_FALLBACK_SEC` | `DEFAULT_AMD_FALLBACK_SEC` | `5.0` | Fallback AMD (s) si campaña con AMD y no hay `OML:AMD_CONF` / muestra |
| `DIALER_AMD_CONF_CACHE_TTL_SEC` | `AMD_CONF_CACHE_TTL_SEC` | `60` | TTL cache lectura `OML:AMD_CONF` / `OML:CAMP.AMD` |
| `DIALER_P_LIB_REMAINING_EPS` | `P_LIB_REMAINING_EPS` | `1.0` | Piso (segundos) del tiempo residual de ocupación en `P_lib`; evita `remaining ≤ 0` |
| `DIALER_GAMMA_THROTTLE_FLOOR` | `GAMMA_THROTTLE_FLOOR` | `0.2` | Valor al que interpola γ en el borde `D → D_max` (piso soft / soft-hold) |
| `DIALER_PACING_SNAPSHOT_TTL_SEC` | `PACING_SNAPSHOT_TTL_SEC` | `30` | TTL de `CAMP:{id}:PACING` (último snapshot de decisión predictiva) |
| `DIALER_THROTTLE_STREAK_K` | `THROTTLE_STREAK_K` | `5` | Ticks consecutivos con `D ≥ D_max` antes de latchear el kill-switch |
| `DIALER_THROTTLE_EXIT_RATIO` | `THROTTLE_EXIT_RATIO` | `0.8` | Histéresis: salir del latch solo si `D < ratio × D_max` |

Constante relacionada **no configurable por env** (hardcodeada en
`naive.py`): `P_HIT_ALPHA = 0.1` (α del EWMA de `P_HIT`). La latencia AMD
media se escribe en `METRICS.AMD_TIME` vía `AmdLatency` (H7).

Otras variables del entorno que acotan el pacing (no exclusivas del predictivo):

| Env var | Default test-env | Efecto |
|---------|------------------|--------|
| `DIALER_CAPS` | `1` | Originaciones máximas por segundo por campaña en el loop |
| `DIALER_TIME_BETWEEN_CALLS` | `3` | Sleep del loop (s) en modos no predictivos sin contactos |
| `DIALER_CALLS_PHASE_TTL_SEC` | `14400` | TTL de las claves de fase `OML:CALLS:PHASE:*` |
| `DIALER_RESERVE_GRACE_SEC` | `30` | Gracia de la reserva de canal antes de originar |
| `DIALER_CHANNEL_AUDIT_INTERVAL_SEC` | `65` | Cadencia del audit de canales que corrige drift de fases |

### 4.2 Parámetros por campaña (Postgres `omnidialer.campaign`)

| Columna | Tipo | Uso en el pacing |
|---------|------|------------------|
| `initial_predictive_model` | bool | Gate del modo predictivo (se sincroniza desde la config de la campaña en OML) |
| `initial_boost_factor` | decimal | Progresivo: multiplicador sobre `A_free`. Predictivo: **aggressiveness** (techo de γ, clamp `[0.1, 5.0]`) |
| `max_channels` | int | Cap duro de canales simultáneos de la campaña |
| `strategy`, `wait`, `maxlen` | — | Config de la cola ACD asociada; `wait` es el timeout de cola (afecta `EXIT_TIMEOUT`, que cuenta como abandono) |

Ambas lecturas (`initial_predictive_model`, `initial_boost_factor`) tienen
cache de 600 s en el worker: cambios desde la UI tardan hasta 10 min en
reflejarse en el pacing salvo reinicio del worker.

---

## 5. Estado en Redis (DB3, salvo indicación)

| Clave | Tipo | Contenido |
|-------|------|-----------|
| `CAMP:{id}:METRICS` | HASH | `P_HIT` (EWMA), `P_HIT_RATIO`, `DROP_RATE` (acumulado), `DROP_RATE_EWMA` (fuente de γ), `HIT_COUNT`, `FAIL_COUNT`, `ABANDON_COUNT`, `CONNECT_COUNT`, `AMD_TIME` (media H7), `WINDOW_MODE` (`ewma_symmetric`) |
| `CAMP:{id}:AMD_LATENCY` | HASH | `AMD_SUM`, `AMD_COUNT`, `AMD` (media de `amd_duration` ACD) |
| `CAMP:{id}:ATT` | HASH | `ATT_SUM`, `ATT_COUNT` (contador del warm-up), `ATT` |
| `CAMP:{id}:ACW` | HASH | `ACW_SUM`, `ACW_COUNT`, `ACW` |
| `CAMP:{id}:AHT` | HASH | `AHT` (derivado; fallback `ATT + ACW`) |
| `CAMP:{id}:ART` | HASH | `ART_SUM`, `ART_COUNT`, `ART` |
| `CAMP:{id}:CHANNELS` | HASH | `RINGING`, `WAITING_AGENT`, `ONCALL` (fases de canal dialer) |
| `CAMP:{id}:PACING` | HASH | Snapshot del último tick predictivo (`MODE`, `REASON`, `GAMMA`, `C_DIAL`, `P_HIT`, `DROP_RATE`, `A_FREE`, `A_EXPECTED`, `C_RINGING`, `THROTTLE_STREAK`, `THROTTLE_LATCHED`, `EVENT`, `TS`); TTL `DIALER_PACING_SNAPSHOT_TTL_SEC` |
| `CAMP:{id}:THROTTLE_STREAK` | STRING | Contador de ticks consecutivos con `D ≥ D_max` |
| `CAMP:{id}:THROTTLE_LATCH` | STRING | `1` si el kill-switch está latcheado |
| `OML:CALLS:{id}:DIALER` | STRING | Total de canales/reservas en vuelo (invariante: = suma de fases) |
| `OML:CALLS:PHASE:{camp}:{contact}[:{callid}]` | STRING | Fase por contacto (TTL `DIALER_CALLS_PHASE_TTL_SEC`) |
| `OML:AGENT:{id}` (DB0) | HASH | `STATUS`, `TIMESTAMP` (snapshot de agentes) |
| `CAMP:{id}:CUSTOMDIALERDST` / `CAMP:{id}:VOICEBOT` (DB3) | STRING | Gate del modo power |

---

## 6. Ejemplo numérico

Campaña warmedeada (`ATT_COUNT ≥ 50`), con:

- `A_free = 2`, `A_expected = 1.4` (3 agentes ONCALL cerca de fin de AHT)
- `C_ringing = 4`, `P_hit = 0.5`
- `DROP_RATE_EWMA = 0.01`, `D_max = 0.03` → zona sana (`0.01 ≤ 0.015`) →
  `γ = aggressiveness = 1.0`

```text
C_dial = floor( (2 + 1.4 − 4×0.5) / 0.5 × 1.0 )
       = floor( (1.4) / 0.5 ) = floor(2.8) = 2
```

Si `DROP_RATE_EWMA` sube a `0.0225` (75% de `D_max`), γ interpola a la mitad del
tramo de freno: `γ = 1.0 + 0.5 × (0.2 − 1.0) = 0.6` →
`C_dial = floor(2.8 × 0.6) = 1`.

Si `DROP_RATE_EWMA ≥ 0.03` durante `K` ticks consecutivos se engarza el
kill-switch (`THROTTLE_LATCH`): `γ = 0`, modo `throttled` (progresivo
estricto) hasta que `D < 0.8 × D_max`. Mientras tanto, con streak `< K`, el
pacer usa soft hold (`γ = GAMMA_THROTTLE_FLOOR`). Con `α=0.1`, un abandono
aislado lleva `D≈0.1`; sin K consecutivos no latchea de inmediato.

---

## 7. Observabilidad

Cada tick de `_allowed_parallel_predictive` (incluidos warm-up / throttled /
fallback) escribe `CAMP:{id}:PACING` en Redis DB3 con TTL corto
(`DIALER_PACING_SNAPSHOT_TTL_SEC`, default 30s). Campos: `MODE`, `REASON`,
`GAMMA`, `C_DIAL`, `P_HIT`, `DROP_RATE` (fuente de γ), `A_FREE`, `A_EXPECTED`,
`C_RINGING`, `THROTTLE_STREAK`, `THROTTLE_LATCHED`, `EVENT`
(`THROTTLE_ENGAGED` / `THROTTLE_CLEARED` solo en transiciones), `TS`. Lectura
tipada desde Django: `OmnidialerService.obtener_pacing_campana` (UI de detalle
aún diferida).

Con `DIALER_PYTHON_LOGLEVEL=debug`, cada tick predictivo también loguea la
decisión completa en los workers `handle-campaign`:

```text
Campaign 5: PREDICTIVE params a_free=2 total_ready=2 a_busy=3.0 a_expected=1.4
t_ring=15.0 aht=95.0 att=80.0 acw=15.0 ... channels ringing=4 waiting_agent=1
oncall=3 total=8 att_count=120 warm_up_sample=50 max_abandon_rate=0.03
drop_rate=0.01 p_hit=0.5 p_hit_ratio=0.48 hit=60 fail=65 abandon=1
gamma=1.0 aggressiveness=1.0 c_dial_raw=2 final_allowed=2 ...
```

y los kill-switch como
`Campaign 5: PREDICTIVE_THROTTLED reason=throttle_streak ...; progressive R=1`
más `WARNING` al engarzar (`THROTTLE_ENGAGED`).

Para inspección manual:

```bash
redis-cli -n 3 HGETALL CAMP:5:METRICS
redis-cli -n 3 HGETALL CAMP:5:CHANNELS
redis-cli -n 3 HGETALL CAMP:5:PACING
redis-cli -n 3 GET CAMP:5:THROTTLE_STREAK
redis-cli -n 3 GET CAMP:5:THROTTLE_LATCH
redis-cli -n 3 HGETALL CAMP:5:ART
```

---

## 8. Referencias de código y tests

| Tema | Path |
|------|------|
| Loop y dispatch de modos | `workers/handle-campaign/src/handler/naive.py` → `process_campaign_inside`, `allowed_parallel_contact_attempts`, `resolve_dial_mode` |
| Pacing predictivo | `naive.py` → `_allowed_parallel_predictive`; helpers puros en `workers/handle-campaign/src/handler/predictive_pacer.py` (`compute_gamma`, `compute_c_dial`, `decide_predictive_pace`, `apply_channel_caps`) |
| `A_expected` / `P_lib` / `t_ring` | `naive.py` → `get_campaign_agent_snapshot`, `compute_a_expected`, `get_campaign_a_expected`, `get_campaign_t_ring`, `compute_agent_p_lib` |
| Métricas Hit/Fail/Abandon | `naive.py` → `_UPDATE_CAMPAIGN_HIT_LUA`, `update_campaign_hit`, `process_event` |
| Settings y defaults | `workers/handle-campaign/src/settings/default.py` |
| Tests del pacer | `workers/handle-campaign/src/tests.py` → `PredictiveConstantsTests`, `PredictivePacerUnitTests`, tests de dispatch de modo |
| Épica de diseño (H1–H9) | `fpignataro/predictivo.md` (repo omldeploytool) |
