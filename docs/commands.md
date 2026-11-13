# Comandos de debug y troubleshooting — OMniDialer

Runbook operativo de comandos usados en prod (Podman + Quadlet) para monitorear pacing predictivo, contadores Redis DB3, Gearman y AMD.

Valores de ejemplo (tenant **Konecta Porta PXQ**):

| Recurso | Valor ejemplo |
|---------|----------------|
| Host dialer workers | `konectaportapxq-dialer-workers` (`10.53.96.9`) |
| Contenedor típico | `dialer-process-campaign-1` |
| Gearman | `10.53.96.14:4730` |
| Nodo ACD (audit) | `10.53.96.13` |
| Campañas dialer | `5`, `6`, `7` |
| Redis dialer | DB **3** (`OML:CALLS:*`, `CAMP:*`) |
| Redis OML | DB **0** (`OML:AGENT:*`, `OML:AMD_CONF:*`, `OML:CAMP:*`) |

Ajustá IPs, nombres de contenedor y IDs de campaña al tenant.

---

## 1. Dashboard live (agentes + campañas + Gearman)

Tablero cada 2 s: ocupación de agentes, `CALLS` vs Asterisk (foto ACD), fases, pacing (`MODE` / `GAMMA` / `P_HIT` / `DROP_EWMA` / `C_DIAL`), contadores de negocio y colas Gearman.

- Podman: Ejecutar en el host de **dialer-workers**:

```bash
watch -n 2 'sudo podman exec -i dialer-process-campaign-1 python3 -c "
import socket, time
from handler.naive import AverageWorker as W
W.connect_redis_dialer()
W.connect_redis_oml()
rd, ro = W.REDIS_DIALER_CONNECTION, W.REDIS_OML_CONNECTION
now = int(time.time())

by_st, ready_waits = {}, []
for k in ro.scan_iter(match=\"OML:AGENT:*\", count=300):
    st, ts = ro.hmget(k, \"STATUS\", \"TIMESTAMP\")
    st = st or \"EMPTY\"
    if st not in (\"OFFLINE\", \"EMPTY\"):
        by_st[st] = by_st.get(st, 0) + 1
        if st == \"READY\" and ts:
            ready_waits.append(max(0, now - int(ts)))

oncall = by_st.get(\"ONCALL\", 0) + by_st.get(\"DIALING\", 0)
acw = by_st.get(\"PAUSE-ACW\", 0) + by_st.get(\"POSTCALL\", 0)
ready = by_st.get(\"READY\", 0)
productive_pool = oncall + acw + ready
occ_pct = (100.0 * (oncall + acw) / productive_pool) if productive_pool > 0 else 0.0
max_r = max(ready_waits) if ready_waits else 0
avg_r = int(sum(ready_waits)/len(ready_waits)) if ready_waits else 0

print(\"=== AGENTES: Ocupacion=%.1f%% (ONCALL/DIAL=%s ACW=%s READY=%s) | Espera READY: prom=%ss max=%ss | Todos=%s\" % (
    occ_pct, oncall, acw, ready, avg_r, max_r, by_st))

ast_ch, ast_ring = {}, {}
for n in rd.smembers(\"OML:ACD:NODES\"):
    if rd.get(\"OML:ACD:NODE:%s:HB\" % n):
        for k, v in (rd.hgetall(\"OML:ACD:%s:CHANNELS\" % n) or {}).items():
            ast_ch[k] = ast_ch.get(k, 0) + int(v)
        for k, v in (rd.hgetall(\"OML:ACD:%s:RINGING\" % n) or {}).items():
            ast_ring[k] = ast_ring.get(k, 0) + int(v)

print(\"\")
print(\"CAMP | CALLS (Ast) | RING/WAIT/ONC (AstR) | Q_ACD | MODE               | GAMMA | P_HIT | DROP_EWMA | A_FR/A_EXP | C_DIAL | ATTEMPT | ANS_PSTN | AMD    | ANS_AG | ABAND\")
print(\"-\" * 165)
for cid in sorted(rd.smembers(\"campaigns:active\")):
    calls = rd.get(\"OML:CALLS:%s:DIALER\" % cid) or 0
    maxc = W.get_campaign_max_available_channels(int(cid))
    ch = rd.hgetall(\"CAMP:%s:CHANNELS\" % cid)
    p = rd.hgetall(\"CAMP:%s:PACING\" % cid)
    m = rd.hgetall(\"CAMP:%s:METRICS\" % cid)
    c = rd.hgetall(\"CAMP:%s:COUNTER\" % cid)
    q_acd = ro.zcard(\"acd:queue:%s:waiting\" % cid)
    print(\"%4s | %5s (%3d) | %4s/%4s/%3s (%4d) | %5d | %-18s | %5.2f | %5.3f | %9.4f | %4s/%5.2f | %6s | %7s | %8s | %6s | %6s | %5d\" % (
        cid,
        str(calls) + \"/\" + str(maxc),
        ast_ch.get(cid, 0),
        ch.get(\"RINGING\", 0), ch.get(\"WAITING_AGENT\", 0), ch.get(\"ONCALL\", 0),
        ast_ring.get(cid, 0),
        q_acd,
        p.get(\"MODE\", \"-\"),
        float(p.get(\"GAMMA\", 0) or 0),
        float(p.get(\"P_HIT\", 0) or 0),
        float(m.get(\"DROP_RATE_EWMA\", 0) or 0),
        p.get(\"A_FREE\", 0),
        float(p.get(\"A_EXPECTED\", 0) or 0),
        p.get(\"C_DIAL\", 0),
        c.get(\"ATTEMPTED_CALLS\", 0),
        c.get(\"ANSWERED_PSTN\", 0),
        c.get(\"AMD\", 0),
        c.get(\"ANSWERED_AGENT\", 0),
        int(c.get(\"EXIT_ABANDON\", 0) or 0) + int(c.get(\"EXIT_TIMEOUT\", 0) or 0),
    ))

s = socket.create_connection((\"10.53.96.14\", 4730), timeout=2)
s.sendall(b\"status\\n\")
gm = [l.split(\"\\t\") for l in s.recv(4096).decode().splitlines() if \"\\t\" in l]
s.close()
queued = {x[0]: \"%s(run=%s/w=%s)\" % (x[1], x[2], x[3]) for x in gm
          if int(x[1]) > 0 or x[0] in (
              \"process-contact\", \"process-event\", \"acd-call-processor\",
              \"send-reports\", \"process-campaign\")}
print(\"\")
print(\"=== GEARMAN:\", queued)
"'
```

- docker-compose: Ejecutar en el host de **dialer-workers**:

```
watch -n 2 'docker compose exec -T dialer-process-camp python3 -c "
import socket, time
from handler.naive import AverageWorker as W
W.connect_redis_dialer()
W.connect_redis_oml()
rd, ro = W.REDIS_DIALER_CONNECTION, W.REDIS_OML_CONNECTION
now = int(time.time())

by_st, ready_waits = {}, []
for k in ro.scan_iter(match=\"OML:AGENT:*\", count=300):
    st, ts = ro.hmget(k, \"STATUS\", \"TIMESTAMP\")
    st = st or \"EMPTY\"
    if st not in (\"OFFLINE\", \"EMPTY\"):
        by_st[st] = by_st.get(st, 0) + 1
        if st == \"READY\" and ts:
            ready_waits.append(max(0, now - int(ts)))

oncall = by_st.get(\"ONCALL\", 0) + by_st.get(\"DIALING\", 0)
acw = by_st.get(\"PAUSE-ACW\", 0) + by_st.get(\"POSTCALL\", 0)
ready = by_st.get(\"READY\", 0)
productive_pool = oncall + acw + ready
occ_pct = (100.0 * (oncall + acw) / productive_pool) if productive_pool > 0 else 0.0
max_r = max(ready_waits) if ready_waits else 0
avg_r = int(sum(ready_waits)/len(ready_waits)) if ready_waits else 0

print(\"=== AGENTES: Ocupacion=%.1f%% (ONCALL/DIAL=%s ACW=%s READY=%s) | Espera READY: prom=%ss max=%ss | Todos=%s\" % (
    occ_pct, oncall, acw, ready, avg_r, max_r, by_st))

ast_ch, ast_ring = {}, {}
for n in rd.smembers(\"OML:ACD:NODES\"):
    if rd.get(\"OML:ACD:NODE:%s:HB\" % n):
        for k, v in (rd.hgetall(\"OML:ACD:%s:CHANNELS\" % n) or {}).items():
            ast_ch[k] = ast_ch.get(k, 0) + int(v)
        for k, v in (rd.hgetall(\"OML:ACD:%s:RINGING\" % n) or {}).items():
            ast_ring[k] = ast_ring.get(k, 0) + int(v)

print(\"\")
print(\"CAMP | CALLS (Ast) | RING/WAIT/ONC (AstR) | Q_ACD | MODE               | GAMMA | P_HIT | DROP_EWMA | A_FR/A_EXP | C_DIAL | ATTEMPT | ANS_PSTN | AMD    | ANS_AG | ABAND\")
print(\"-\" * 165)
for cid in sorted(rd.smembers(\"campaigns:active\")):
    calls = rd.get(\"OML:CALLS:%s:DIALER\" % cid) or 0
    maxc = W.get_campaign_max_available_channels(int(cid))
    ch = rd.hgetall(\"CAMP:%s:CHANNELS\" % cid)
    p = rd.hgetall(\"CAMP:%s:PACING\" % cid)
    m = rd.hgetall(\"CAMP:%s:METRICS\" % cid)
    c = rd.hgetall(\"CAMP:%s:COUNTER\" % cid)
    q_acd = ro.zcard(\"acd:queue:%s:waiting\" % cid)
    print(\"%4s | %5s (%3d) | %4s/%4s/%3s (%4d) | %5d | %-18s | %5.2f | %5.3f | %9.4f | %4s/%5.2f | %6s | %7s | %8s | %6s | %6s | %5d\" % (
        cid,
        str(calls) + \"/\" + str(maxc),
        ast_ch.get(cid, 0),
        ch.get(\"RINGING\", 0), ch.get(\"WAITING_AGENT\", 0), ch.get(\"ONCALL\", 0),
        ast_ring.get(cid, 0),
        q_acd,
        p.get(\"MODE\", \"-\"),
        float(p.get(\"GAMMA\", 0) or 0),
        float(p.get(\"P_HIT\", 0) or 0),
        float(m.get(\"DROP_RATE_EWMA\", 0) or 0),
        p.get(\"A_FREE\", 0),
        float(p.get(\"A_EXPECTED\", 0) or 0),
        p.get(\"C_DIAL\", 0),
        c.get(\"ATTEMPTED_CALLS\", 0),
        c.get(\"ANSWERED_PSTN\", 0),
        c.get(\"AMD\", 0),
        c.get(\"ANSWERED_AGENT\", 0),
        int(c.get(\"EXIT_ABANDON\", 0) or 0) + int(c.get(\"EXIT_TIMEOUT\", 0) or 0),
    ))

s = socket.create_connection((\"gearman\", 4730), timeout=2)
s.sendall(b\"status\\n\")
gm = [l.split(\"\\t\") for l in s.recv(4096).decode().splitlines() if \"\\t\" in l]
s.close()
queued = {x[0]: \"%s(run=%s/w=%s)\" % (x[1], x[2], x[3]) for x in gm
          if int(x[1]) > 0 or x[0] in (
              \"process-contact\", \"process-event\", \"acd-call-processor\",
              \"send-reports\", \"process-campaign\")}
print(\"\")
print(\"=== GEARMAN:\", queued)
"'
```

### Qué mirar

| Señal | Sano | Problema |
|-------|------|----------|
| `CALLS` vs `(Ast)` | Diferencia ≤ ~5 | Redis inflado → cupo fantasma |
| `MODE` | `PREDICTIVE` | `PREDICTIVE_THROTTLED` + `C_DIAL=0` |
| `DROP_EWMA` | ≪ `DIALER_MAX_ABANDON_RATE` | Cerca/sobre el techo → latch |
| `P_HIT` | > floor (0.05) con humanos | Clavado en 0.050 + AMD≈ANS_PSTN |
| `process-event` | cola cerca de 0 | Miles en cola → desfase CALLS |

---

## 2. Watch: contadores Redis DB3 (`OML:CALLS`) vs Asterisk

Enfocado en desfase de canales y phase keys huérfanas:

```bash
watch -n 2 'sudo podman exec -i dialer-process-campaign-1 python3 -c "
from handler.naive import AverageWorker as W
W.connect_redis_dialer()
rd = W.REDIS_DIALER_CONNECTION
ast_ch, ast_ring = {}, {}
for n in rd.smembers(\"OML:ACD:NODES\") or []:
    if rd.get(\"OML:ACD:NODE:%s:HB\" % n):
        for k, v in (rd.hgetall(\"OML:ACD:%s:CHANNELS\" % n) or {}).items():
            ast_ch[k] = ast_ch.get(k, 0) + int(v)
        for k, v in (rd.hgetall(\"OML:ACD:%s:RINGING\" % n) or {}).items():
            ast_ring[k] = ast_ring.get(k, 0) + int(v)
print(\"%4s | %5s | %4s | %5s | RING/WAIT/ONC | AstR | phase_keys\" % (\"CAMP\", \"CALLS\", \"Ast\", \"delta\"))
print(\"-\" * 72)
for cid in (\"5\", \"6\", \"7\"):
    calls = int(rd.get(\"OML:CALLS:%s:DIALER\" % cid) or 0)
    ch = rd.hgetall(\"CAMP:%s:CHANNELS\" % cid) or {}
    r = int(ch.get(\"RINGING\", 0) or 0)
    w = int(ch.get(\"WAITING_AGENT\", 0) or 0)
    o = int(ch.get(\"ONCALL\", 0) or 0)
    ast = int(ast_ch.get(cid, 0))
    astr = int(ast_ring.get(cid, 0))
    nphase = sum(1 for _ in rd.scan_iter(match=\"OML:CALLS:PHASE:%s:*\" % cid, count=200))
    print(\"%4s | %5d | %4d | %+5d | %4d/%4d/%3d | %4d | %5d\" % (
        cid, calls, ast, calls - ast, r, w, o, astr, nphase))
"'
```

`delta` positivo grande o `phase_keys ≫ CALLS` → fantasmas / finalize atrasado.

---

## 3. Watch: colas Gearman (process-event)

```bash
watch -n 2 'sudo podman exec -i dialer-process-campaign-1 python3 -c "
import socket
s=socket.create_connection((\"10.53.96.14\",4730),2)
s.sendall(b\"status\\n\")
raw=s.recv(8192).decode()
s.close()
for line in raw.splitlines():
    if not line.strip() or \"\\t\" not in line:
        continue
    p=line.split(\"\\t\")
    name, queued, running, workers = p[0], p[1], p[2], p[3]
    if name in (\"process-event\",\"process-campaign\",\"process-contact\",
                \"acd-call-processor\",\"send-reports\") or int(queued)>0:
        print(\"%s\\tqueued=%s\\trun=%s\\tw=%s\" % (name, queued, running, workers))
"'
```

Snapshot único (sin watch):

```bash
sudo podman exec -i dialer-process-campaign-1 python3 -c "
import socket
s=socket.create_connection(('10.53.96.14',4730),2)
s.sendall(b'status\n')
print(s.recv(8192).decode())
s.close()
"
```

Columnas Gearman: `job  queued  running  workers`. Reanudar campañas solo con `process-event` preferiblemente **&lt; ~100**.

### Guardrails P0 (env dialer / ACD)

Con `DIALER_ORIGIN_GUARDRAILS_ENABLED=true` (default), `process-campaign` **aborta el lote CAPS** si:

| Variable | Default | Efecto |
|----------|---------|--------|
| `DIALER_GUARDRAIL_PROCESS_EVENT_QUEUED_MAX` | 100 | `process-event` queued ≥ N |
| `DIALER_GUARDRAIL_CALLS_AST_DELTA_MAX` | 15 | `OML:CALLS − Ast` &gt; N |
| `DIALER_GUARDRAIL_PHASE_CALLS_DELTA_MAX` | 20 | `phase TOTAL − CALLS` &gt; N |
| `DIALER_GLOBAL_CAPS` | 0 (off) | Techo originates/s tenant-wide |
| `DIALER_REQUIRE_ENDPOINTS` | false | Worker exit si Redis/Gearman unreachable al boot |

ACD: `AMD_LATENCY_QUEUE_MAX` (default 100) omite jobs `AmdLatency` bajo backlog; Dial AMD MACHINE lleva `amd_duration` embebido. Tras Dial AMD el ACD **no** reenvía `ChannelDestroyed` a `process-event` (cupo ya liberado en el dialer). Ver [adr-dial-attempt-contract.md](adr-dial-attempt-contract.md).

### Wrapup cooldown post-colgado (ACD)

Tras hangup humano el ACD setea `acd:cooldown:agent:{id}` con:

`TTL = max(WRAPUPTIME, AUTO_UNPAUSE) + AGENT_WRAPUP_POST_READY_GRACE_SEC`

| Fuente | Dónde |
|--------|--------|
| `WRAPUPTIME` | `OML:CAMP:{id}` (cola campaña; regenerar_asterisk) |
| `AUTO_UNPAUSE` | `OML:CAMP:{id}` si `set_auto_unpause` |
| `AGENT_WRAPUP_POST_READY_GRACE_SEC` | env ACD (default **5**; `0` = sin gracia) |

El claim/reserve no ofrece mientras exista la clave (aunque `STATUS=READY`). El dialer **no** cuenta esos READY en `a_free`.

```bash
# Ver cooldown de un agente
redis-cli -n 0 TTL acd:cooldown:agent:1
redis-cli -n 0 GET acd:cooldown:agent:1
# WRAPUPTIME / AUTO_UNPAUSE de campaña
redis-cli -n 0 HMGET OML:CAMP:5 WRAPUPTIME AUTO_UNPAUSE
```

Para más aire **después** del ACW de consola: subir `wrapuptime` de la cola por encima de `auto_unpause` del grupo (p. ej. 60 vs 45).

---

## 4. Foto ACD en Redis DB3 + CALLS/fases

```bash
sudo podman exec -i dialer-process-campaign-1 python3 -c "
from handler.naive import AverageWorker as W
W.connect_redis_dialer()
rd=W.REDIS_DIALER_CONNECTION
for n in sorted(rd.smembers('OML:ACD:NODES') or []):
    print('NODE', n, 'HB', rd.get('OML:ACD:NODE:%s:HB' % n))
    print('  CH', rd.hgetall('OML:ACD:%s:CHANNELS' % n))
    print('  RING', rd.hgetall('OML:ACD:%s:RINGING' % n))
for cid in ('5','6','7'):
    print(cid, 'CALLS', rd.get('OML:CALLS:%s:DIALER' % cid),
          'PHASES', rd.hgetall('CAMP:%s:CHANNELS' % cid))
"
```

`CH {}` con HB fresco y campañas pausadas = sin canales dialer en Asterisk (normal).

---

## 5. Config AMD (Redis OML DB0 + latencia dialer)

```bash
sudo podman exec -i dialer-process-campaign-1 python3 -c "
from handler.naive import AverageWorker as W
W.connect_redis_oml()
ro = W.REDIS_OML_CONNECTION

print('=== OML:AMD_CONF:* ===')
keys = sorted(ro.scan_iter(match='OML:AMD_CONF:*', count=50))
if not keys:
    print('(ninguna key)')
for k in keys:
    print(k, ro.hgetall(k))

print()
print('=== AMD por campaña (OML:CAMP:{id}.AMD = detectar_contestadores) ===')
for cid in (5, 6, 7):
    camp = ro.hgetall('OML:CAMP:%s' % cid) or {}
    print('CAMP %s: AMD=%r NAME=%r' % (cid, camp.get('AMD'), camp.get('NAME')))

print()
print('=== Latencia AMD media (dialer DB3) ===')
W.connect_redis_dialer()
rd = W.REDIS_DIALER_CONNECTION
for cid in (5, 6, 7):
    print(cid, 'AMD_LATENCY', rd.hgetall('CAMP:%s:AMD_LATENCY' % cid))
    print(cid, 'METRICS.AMD_TIME', rd.hget('CAMP:%s:METRICS' % cid, 'AMD_TIME'))
"
```

Cambios de AMD: UI Django (`AmdConf`) + sync/regeneración; no editar solo Redis (se pisa).

---

## 6. Workaround: limpiar throttle latch (campañas clavadas)

Cuando `MODE=PREDICTIVE_THROTTLED`, `GAMMA=0`, `C_DIAL=0` y `DROP_EWMA` no baja solo del umbral de salida (`THROTTLE_EXIT_RATIO * MAX_ABANDON_RATE`):

```bash
sudo podman exec -i dialer-process-campaign-1 python3 -c "
import handler.naive as n
from handler.naive import AverageWorker as W
W.connect_redis_dialer()
r = W.REDIS_DIALER_CONNECTION
dmax = float(n.MAX_ABANDON_RATE)
seed = round(0.90 * dmax, 4)
print('MAX_ABANDON_RATE', dmax, 'exit', round(0.8 * dmax, 4), 'seed', seed)
for cid in (5, 6, 7):
    print(cid, 'boost', W.get_boost_factor(cid),
          'latch_ttl', r.ttl('CAMP:%s:THROTTLE_LATCH' % cid),
          'drop', (r.hget('CAMP:%s:METRICS' % cid, 'DROP_RATE_EWMA') or ''))
    r.delete('CAMP:%s:THROTTLE_LATCH' % cid, 'CAMP:%s:THROTTLE_STREAK' % cid)
    r.hset('CAMP:%s:METRICS' % cid, 'DROP_RATE_EWMA', seed)
    print(cid, 'latch cleared')
"
```

Ajustá la tupla de IDs. Tras el clear: en el dashboard `MODE=PREDICTIVE` y `C_DIAL` puede subir; si el drop vuelve sobre el techo varios ticks, se reclava.

---

## 7. Workaround: alinear `OML:CALLS` a la foto ACD

**Orden:** pausar campañas → align → esperar `process-event` &lt; ~100 → reanudar.

No poner a `0` a ciegas con campañas activas y cola de eventos: los DECR pendientes + `CAPS` regeneran la ráfaga.

```bash
sudo podman exec -i dialer-process-campaign-1 python3 -c "
from handler.naive import AverageWorker as W
W.connect_redis_dialer()
rd = W.REDIS_DIALER_CONNECTION
n = '10.53.96.13'
ch = {k: int(v) for k, v in (rd.hgetall('OML:ACD:%s:CHANNELS' % n) or {}).items()}
rg = {k: int(v) for k, v in (rd.hgetall('OML:ACD:%s:RINGING' % n) or {}).items()}
print('ACD CH', ch, 'RING', rg)
for cid in ('5', '6', '7'):
    total = int(ch.get(cid, 0))
    ringing = int(rg.get(cid, 0))
    oncall = max(0, total - ringing)
    print(cid, 'before', rd.get('OML:CALLS:%s:DIALER' % cid),
          rd.hgetall('CAMP:%s:CHANNELS' % cid))
    rd.set('OML:CALLS:%s:DIALER' % cid, total)
    rd.hset('CAMP:%s:CHANNELS' % cid, mapping={
        'RINGING': ringing, 'WAITING_AGENT': 0, 'ONCALL': oncall,
    })
    purged = 0
    for k in list(rd.scan_iter(match='OML:CALLS:PHASE:%s:*' % cid, count=200)):
        rd.delete(k)
        purged += 1
    for k in list(rd.scan_iter(match='OML:CALLS:RESERVE_TS:%s:*' % cid, count=200)):
        rd.delete(k)
    W._publish_calls_count(int(cid))
    print(cid, 'after', total, {'RINGING': ringing, 'ONCALL': oncall},
          'purged_phase', purged)
print('done')
"
```

---

## 8. Escalar réplicas `process-event` (Quadlet)

Plantilla: `/etc/omnileads/dialer/quadlet/dialer_process_event@.container`  
Instancias: symlinks `/etc/containers/systemd/dialer_process_event@N.container`

Ejemplo: subir a **10** (si ya existen 1–6):

```bash
for i in 7 8 9 10; do
  sudo ln -sfn /etc/omnileads/dialer/quadlet/dialer_process_event@.container \
    /etc/containers/systemd/dialer_process_event@${i}.container
done
sudo systemctl daemon-reload
for i in 7 8 9 10; do
  sudo systemctl start dialer_process_event@${i}.service
done
sudo podman ps --filter name=dialer-process-event --format '{{.Names}} {{.Status}}'
```

Persistir en inventário/tenant: `dialer_process_event_replicas: N` y `./deploy.sh --action=update …` (tag dialer).

**Nota:** más workers en una VM con pocos vCPU puede subir el load sin bajar la cola si las campañas siguen originando a `CAPS` alto. Preferir bajar CAPS/boost o subir CPU.

---

## 9. Workers process-event: estado y logs

```bash
sudo podman ps --filter name=dialer-process-event --format '{{.Names}} {{.Status}}'
sudo podman logs --tail 40 dialer-process-event-1
sudo podman logs --tail 40 dialer-process-event-2
sudo podman logs --tail 40 dialer-process-event-3
```

Warnings frecuentes bajo ráfaga AMD:

```text
Campaign N: finalize without phase key (orphan) contact=… ctx=Dial AMD
```

El finalize igual intenta `DECR` de `OML:CALLS`; el warning indica carrera / phase ya borrada.

---

## 10. Asterisk (host ACD): canales reales

En el host ACD (contenedor `acd-server`):

```bash
sudo podman exec -i acd-server asterisk -rx 'core show channels count'
sudo podman exec -i acd-server asterisk -rx 'core show channels concise' | head -100
sudo podman exec -i acd-server asterisk -rx 'core show channels concise' | grep -E 'id_camp:|Dialer|to_pstn' | head -50
```

Comparar con `OML:ACD:{nodo}:CHANNELS` (audit) y `OML:CALLS:{camp}:DIALER`.

---

## 11. Salud de la VM dialer-workers

```bash
uptime
nproc
free -h
df -h /
sudo podman stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' \
  | grep -E 'event|campaign|contact|gearman|redis' || sudo podman stats --no-stream
```

Load ≫ `nproc` + CPU de `dialer-process-event-*` alto → cola Gearman lenta y desfase Redis/Ast.

---

## 12. Lectura rápida de `dialer.env` relevante

| Variable | Rol |
|----------|-----|
| `CAPS` | Máx. originates por campaña por ventana ~1 s |
| `TIME_BETWEEN_CALLS` | Sleep idle; **no** frena el lote CAPS |
| `DIALER_MAX_ABANDON_RATE` | Techo para γ / kill-switch |
| `DIALER_CHANNEL_AUDIT_INTERVAL_SEC` | Periodo audit dialer vs foto ACD (default 60) |
| `DIALER_RESERVE_GRACE_SEC` | Gracia reservas recientes (audit no baja agresivo) |
| `PYTHON_LOGLEVEL` | En prod preferir `warning`/`info`; `debug` satura CPU |
| Boost campaña (`initial_boost_factor`) | Techo de γ en zona sana (UI / Postgres dialer) |

---

## 13. Secuencia de emergencia típica

1. Confirmar síntoma: dashboard → `CALLS≫Ast`, `process-event` alto, y/o `THROTTLED`.
2. **Pausar** campañas afectadas.
3. **Align** CALLS/fases a foto ACD (§7).
4. Esperar `process-event` &lt; ~100 (§3).
5. Opcional: clear throttle (§6) si quedaron latched.
6. Reanudar con **CAPS bajo** (p. ej. 5–10) y boost ~1.0 hasta estabilizar.
7. Si la VM está chica: subir vCPU antes de subir réplicas event.

---

## Referencias

- `docs/dialer_modes.md` — modos de marcado
- `docs/informe.md` — arquitectura / deuda
- `CLAUDE.md` — mapa del repo dialer
- Código: `workers/handle-campaign/src/handler/naive.py`, `handler/predictive_pacer.py`
