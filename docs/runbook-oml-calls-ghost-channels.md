# Runbook: canales fantasma en OML:CALLS

## Síntoma

`GET OML:CALLS:{camp_id}:DIALER` en Redis DB3 muestra un valor > 0 mientras:

- No hay canales dialer PSTN activos en Asterisk (ARI `list_channels`), o
- La campaña está `FINALIZED` / `PAUSED` desde hace varios minutos.

El contador refleja **reservas** del dialer (INCR antes de `process-contact`), no canales físicos.

## Diagnóstico rápido

1. Redis: `GET OML:CALLS:{id}:DIALER`
2. Postgres dialer: `SELECT dialer_status FROM campaign WHERE id = {id}`
3. Logs `acd-app`: `ChannelDestroyed` sin metadata, `originate failed`, `ORIGINATE_FAILED`
4. Logs `dialer-process-event`: `process_event`, `ChannelDestroyed ignorado`, `ORIGINATE_FAILED`
5. Logs `dialer-scheduler`: `Audit corrected camp`

## Mitigaciones desplegadas

| Mecanismo | Componente | Efecto |
|-----------|------------|--------|
| `ORIGINATE_FAILED` | acd-app → process-event | Decremento si originate PSTN falla |
| Reset al finalizar | stop_campaign / auto-FINALIZED | `SET OML:CALLS:{id}:DIALER 0` |
| Metadata Redis | acd-app `acd:pending_dial:*` TTL 7200s | Multi-nodo + llamadas largas |
| Decremento idempotente | `OML:CALLS:DECR:{camp}:{contact}:{callid}` | Sin doble DECR |
| Audit 60s | dialer-scheduler → `audit-dialer-channels` (ACD) | Autocuración |
| Shutdown acd-app | `flush_pending_dialer_decrements_on_shutdown` | Reinicios planificados |

## Alertas recomendadas

1. **Campaña inactiva con reservas**: `OML:CALLS:{id}:DIALER > 0` y `dialer_status != ACTIVE` por > 5 min.
2. **Audit masivo**: más de N líneas `Audit corrected camp` en un ciclo de 60 s (umbral sugerido: 3).
3. **Gearman process-event**: errores `failed after 3 attempts` en `LegacyEventForwarder`.

## Corrección manual

```bash
# Ver valor actual
redis-cli -n 3 GET OML:CALLS:74:DIALER

# Si campaña finalizada y Asterisk en 0
redis-cli -n 3 SET OML:CALLS:74:DIALER 0
# Publicar a supervisión (desde dialer o script equivalente a _publish_calls_count)
```

Esperar un ciclo de audit (≤ 60 s) antes de intervención manual si el scheduler está activo.

## Correlación post-reinicio

Tras reinicio de `acd-app` o `systemd-data_statefull`:

- Ventana típica de fantasmas: 0–60 s hasta próximo audit.
- Buscar en logs ACD: `pending_dial` pop vacío, `ORIGINATE_FAILED` en shutdown.
- Buscar jobs fallidos en Postgres `jobs` (dialer) en la misma ventana temporal.

## Variables de entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `PENDING_DIAL_TTL_SEC` | 7200 | TTL metadata pending_dial (ACD) |
| `DIALER_CHANNEL_AUDIT_INTERVAL_SEC` | 60 | Intervalo audit scheduler |
| `DIALER_CALLS_DECR_DEDUP_TTL_SEC` | 3600 | TTL clave idempotencia DECR |
| `DIALER_RESERVE_GRACE_SEC` | 30 | Gracia audit tras INCR reciente |
