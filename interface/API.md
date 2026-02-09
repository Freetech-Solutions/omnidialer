# Documentación de la API - Dialer Interface

## Descripción General

La API del Dialer Interface es una interfaz REST basada en Flask que proporciona endpoints para gestionar campañas de marcado (dialer) de manera multi-canal. La implementación utiliza Gearman para escalabilidad horizontal, permitiendo distribuir las tareas de procesamiento entre múltiples workers.

### Arquitectura

- **Framework**: Flask
- **Sistema de colas**: Gearman
- **Clase principal**: `GearmanDialer` (implementa la clase abstracta `Dialer`)
- **Puerto por defecto**: 1440

---

## Endpoints de la API

### Gestión de Campañas

#### 1. Crear Campaña

Crea una nueva campaña de marcado.

**Endpoint**: `POST /create-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a crear

**Body (JSON)**:
```json
{
  "contact-strategy": ["array", "de", "estrategias"],
  "prefix": "prefijo_opcional" // o null/[] para omitir
}
```

**Respuesta**: JSON con el resultado de la operación (decodificado desde Gearman)

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/create-campaign/123 \
  -H "Content-Type: application/json" \
  -d '{"contact-strategy": ["voip", "email"], "prefix": "+34"}'
```

---

#### 2. Editar Campaña

Edita una campaña existente.

**Endpoint**: `POST /edit-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a editar

**Body (JSON)**:
```json
{
  "contact-strategy": ["array", "de", "estrategias"]
}
```

**Respuesta**: JSON con el resultado de la operación

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/edit-campaign/123 \
  -H "Content-Type: application/json" \
  -d '{"contact-strategy": ["voip", "sms"]}'
```

---

#### 3. Iniciar Campaña

Inicia una campaña de marcado.

**Endpoint**: `POST /start-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a iniciar

**Parámetros de formulario**:
- `sync-omnileads` (opcional): Valores aceptados: `'1'`, `'true'`, `'t'`, `'yes'`, `'y'`, `'on'` (case-insensitive). Por defecto: `false`

**Respuesta**: 
```json
{"msg": "Campaign process to be started"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/start-campaign/123 \
  -d "sync-omnileads=true"
```

---

#### 4. Detener Campaña

Detiene una campaña en ejecución.

**Endpoint**: `POST /stop-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a detener

**Parámetros de formulario**:
- `sync-omnileads` (opcional): Valores aceptados: `'1'`, `'true'`, `'t'`, `'yes'`, `'y'`, `'on'` (case-insensitive). Por defecto: `false`

**Respuesta**: 
```json
{"msg": "Campaign finalized"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/stop-campaign/123 \
  -d "sync-omnileads=false"
```

---

#### 5. Pausar Campaña

Pausa temporalmente una campaña en ejecución.

**Endpoint**: `POST /pause-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a pausar

**Parámetros de formulario**:
- `sync-omnileads` (opcional): Valores aceptados: `'1'`, `'true'`, `'t'`, `'yes'`, `'y'`, `'on'` (case-insensitive). Por defecto: `false`

**Respuesta**: 
```json
{"msg": "Campaign process to be paused"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/pause-campaign/123
```

---

#### 6. Reanudar Campaña

Reanuda una campaña previamente pausada.

**Endpoint**: `POST /resume-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a reanudar

**Respuesta**: 
```json
{"msg": "Campaign process to be resumed"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/resume-campaign/123
```

---

#### 7. Eliminar Campaña

Elimina una campaña del sistema.

**Endpoint**: `POST /delete-campaign/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña a eliminar

**Respuesta**: 
```json
{"msg": "Campaign deleted"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/delete-campaign/123
```

---

### Reglas de Incidencias

#### 8. Agregar Regla de Incidencia con Disposición

Agrega una regla de incidencia asociada a una disposición específica.

**Endpoint**: `POST /add-incidence-rule-disposition/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "id_contact": 456,  // opcional, por defecto: -1
  "disposition_option": 789  // opcional, por defecto: -1
}
```

**Respuesta**: 
```json
{"msg": "Disposition added"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/add-incidence-rule-disposition/123 \
  -H "Content-Type: application/json" \
  -d '{"id_contact": 456, "disposition_option": 789}'
```

---

#### 9. Crear Regla de Incidencia

Crea una nueva regla de incidencia para una campaña.

**Endpoint**: `POST /create-incidence-rule/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "id": 1,                    // opcional, por defecto: -1
  "type": 2,                  // opcional, por defecto: -1
  "status": 3,                // opcional, por defecto: -1
  "status_custom": "texto",   // opcional, por defecto: ""
  "disposition_option_id": 4, // opcional, por defecto: -1
  "max_attempt": 5,           // opcional, por defecto: -1
  "retry_later": 6,           // opcional, por defecto: -1
  "in_mode": 7                // opcional, por defecto: -1
}
```

**Respuesta**: 
```json
{"msg": "Incide rule added"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/create-incidence-rule/123 \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "type": 2,
    "status": 3,
    "max_attempt": 5
  }'
```

---

#### 10. Eliminar Regla de Incidencia

Elimina una regla de incidencia existente.

**Endpoint**: `POST /delete-incidence-rule/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "id": 1,      // ID de la regla a eliminar
  "type": 2     // Tipo de regla
}
```

**Respuesta**: 
```json
{"msg": "Incidence rule deleted"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/delete-incidence-rule/123 \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "type": 2}'
```

---

#### 11. Actualizar Regla de Incidencia

Actualiza una regla de incidencia existente.

**Endpoint**: `POST /update-incidence-rule/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "id": 1,                    // opcional, por defecto: -1
  "type": 2,                  // opcional, por defecto: -1
  "status": 3,                // opcional, por defecto: -1
  "status_custom": "texto",   // opcional, por defecto: ""
  "disposition_option_id": 4, // opcional, por defecto: -1
  "max_attempt": 5,           // opcional, por defecto: -1
  "retry_later": 6,           // opcional, por defecto: -1
  "in_mode": 7                // opcional, por defecto: -1
}
```

**Respuesta**: 
```json
{"msg": "Incidence rule was updated"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/update-incidence-rule/123 \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "type": 2,
    "status": 4,
    "max_attempt": 6
  }'
```

---

### Llamadas Manuales

#### 12. Llamada Manual

Realiza una llamada manual a un número de teléfono específico.

**Endpoint**: `POST /manual-call/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "phone_number": "+34123456789",  // requerido
  "id_agent": 123,                 // requerido
  "id_contact": 456                // opcional
}
```

**Respuesta** (202 Accepted):
```json
{
  "status": "queued",
  "id_campaign": 123,
  "id_contact": 456,
  "id_agent": 123,
  "phone_number": "+34123456789",
  "job": {"queued": true}
}
```

**Errores**:
- `400 Bad Request`: Si falta `phone_number` o `id_agent`

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/manual-call/123 \
  -H "Content-Type: application/json" \
  -d '{
    "phone_number": "+34123456789",
    "id_agent": 123,
    "id_contact": 456
  }'
```

---

#### 13. Llamar Contacto de Campaña

Realiza una llamada a un contacto específico de la campaña.

**Endpoint**: `POST /call-campaign-contact/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "id_agent": 123,              // requerido
  "id_contact": 456,            // opcional
  "force": false,               // opcional, por defecto: false
  "ignore_opening_hours": false // opcional, por defecto: false
}
```

**Respuesta** (202 Accepted):
```json
{
  "status": "queued",
  "id_campaign": 123,
  "id_contact": 456,
  "id_agent": 123,
  "force": false,
  "ignore_opening_hours": false,
  "job": {"queued": true}
}
```

**Errores**:
- `400 Bad Request`: Si falta `id_agent`

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/call-campaign-contact/123 \
  -H "Content-Type: application/json" \
  -d '{
    "id_agent": 123,
    "id_contact": 456,
    "force": true,
    "ignore_opening_hours": false
  }'
```

---

### Agenda y Programación

#### 14. Agregar a Agenda

Programa una llamada para una fecha y hora específica.

**Endpoint**: `POST /add-agenda/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Body (JSON)**:
```json
{
  "datetime": "2024-12-25 10:00:00",  // opcional, por defecto: ""
  "campaign_name": "Mi Campaña",       // opcional, por defecto: ""
  "phone_number": "+34123456789",      // opcional, por defecto: ""
  "id_contact": 456                    // opcional, por defecto: ""
}
```

**Respuesta**: 
```json
{"msg": "Agenda was sent for scheduling"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/add-agenda/123 \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2024-12-25 10:00:00",
    "campaign_name": "Navidad 2024",
    "phone_number": "+34123456789",
    "id_contact": 456
  }'
```

---

### Gestión de Base de Datos

#### 15. Cambiar Base de Datos

Cambia la base de datos asociada a una campaña.

**Endpoint**: `POST /change-database/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Respuesta**: 
```json
{"msg": "Database was changed"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/change-database/123
```

---

### Eventos AMD (Answering Machine Detection)

#### 16. Agregar Evento AMD

Agrega un evento de detección de contestador automático.

**Endpoint**: `POST /add_amd_event`

**Body (JSON)**:
```json
{
  // Datos del evento AMD (estructura específica según implementación)
  // Se agrega automáticamente "disposition_option": -2
}
```

**Respuesta**: 
```json
{"msg": "Disposition added"}
```

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/add_amd_event \
  -H "Content-Type: application/json" \
  -d '{"event_data": "..."}'
```

---

### Endpoints HTMX (Interfaz de Usuario)

Estos endpoints están diseñados para ser utilizados por la interfaz web basada en HTMX.

#### 17. Página Principal

Renderiza la página principal de administración del dialer.

**Endpoint**: `GET /`

**Respuesta**: HTML (template `index.html`)

---

#### 18. Inicialización HTMX

Obtiene los datos iniciales de las campañas para la carga inicial de la página.

**Endpoint**: `GET /htmx/init`

**Respuesta**: HTML renderizado con los datos de las campañas

---

#### 19. Estadísticas de Campaña

Obtiene las estadísticas de una campaña específica.

**Endpoint**: `GET /htmx/stats/<id_campaign>`

**Parámetros de URL**:
- `id_campaign` (int): ID de la campaña

**Respuesta**: HTML renderizado con las estadísticas

**Ejemplo**:
```bash
curl http://localhost:1440/htmx/stats/123
```

---

#### 20. Gestionar Dialer

Ejecuta acciones de gestión del dialer (iniciar, detener, etc.).

**Endpoint**: `POST /htmx/manage-dialer/`

**Parámetros de formulario**:
- `action` (string): Acción a ejecutar

**Respuesta**: HTML/JSON según la acción

**Ejemplo**:
```bash
curl -X POST http://localhost:1440/htmx/manage-dialer/ \
  -d "action=start"
```

---

## Códigos de Estado HTTP

- `200 OK`: Operación exitosa
- `202 Accepted`: Operación aceptada y en cola (para llamadas manuales)
- `400 Bad Request`: Error en los parámetros de la solicitud
- `500 Internal Server Error`: Error interno del servidor

---

## Configuración

### Variables de Entorno

- `GEARMAN_JOB_SERVERS`: Lista de servidores Gearman separados por `|` (ej: `"server1:4730|server2:4730"`)
- `WEBSOCKET_SERVER`: URL del servidor WebSocket para actualizaciones en tiempo real
- `PYTHON_LOGLEVEL`: Nivel de logging (por defecto: `INFO`)

### Ejemplo de Configuración

```bash
export GEARMAN_JOB_SERVERS="localhost:4730|worker1:4730"
export WEBSOCKET_SERVER="ws://localhost:8080/ws"
export PYTHON_LOGLEVEL="DEBUG"
```

---

## Arquitectura Interna

### Clases Principales

#### `Dialer` (Clase Abstracta)
Clase base que define la interfaz común para todas las implementaciones de dialer.

**Métodos abstractos**:
- `create_campaign(id_campaign, contact_strategy)`
- `edit_campaign(id_campaign, contact_strategy)`
- `start_campaign(id_campaign)`
- `stop_campaign(id_campaign)`
- `pause_campaign(id_campaign)`
- `resume_campaign(id_campaign)`
- `delete_campaign(id_campaign)`

#### `GearmanDialer` (Implementación)
Implementación concreta que utiliza Gearman para distribuir tareas.

**Características**:
- Codifica/decodifica payloads en JSON UTF-8
- Envía trabajos a Gearman (síncronos o asíncronos según el caso)
- Soporta sincronización con OmniLeads (`sync_omnileads`)

---

## Notas de Implementación

1. **Trabajos en Background**: Algunas operaciones (como `start-campaign`, `pause-campaign`, `resume-campaign`) se ejecutan en background y retornan inmediatamente.

2. **Sincronización con OmniLeads**: El parámetro `sync-omnileads` permite sincronizar el estado de la campaña con OmniLeads cuando se inicia, detiene o pausa.

3. **Normalización de Parámetros**: El parámetro `sync-omnileads` acepta múltiples formatos y se normaliza a booleano internamente.

4. **Gestión de Errores**: Los errores de validación retornan código 400 con un mensaje JSON descriptivo.

5. **WebSockets**: La interfaz utiliza WebSockets para actualizaciones en tiempo real de las campañas.

---

## Ejemplos de Uso Completos

### Flujo Completo de una Campaña

```bash
# 1. Crear campaña
curl -X POST http://localhost:1440/create-campaign/123 \
  -H "Content-Type: application/json" \
  -d '{"contact-strategy": ["voip"], "prefix": "+34"}'

# 2. Iniciar campaña
curl -X POST http://localhost:1440/start-campaign/123 \
  -d "sync-omnileads=true"

# 3. Realizar llamada manual
curl -X POST http://localhost:1440/manual-call/123 \
  -H "Content-Type: application/json" \
  -d '{
    "phone_number": "+34123456789",
    "id_agent": 123,
    "id_contact": 456
  }'

# 4. Pausar campaña
curl -X POST http://localhost:1440/pause-campaign/123

# 5. Reanudar campaña
curl -X POST http://localhost:1440/resume-campaign/123

# 6. Detener campaña
curl -X POST http://localhost:1440/stop-campaign/123

# 7. Eliminar campaña
curl -X POST http://localhost:1440/delete-campaign/123
```

---

## Troubleshooting

### Problemas Comunes

1. **Error de conexión a Gearman**: Verificar que `GEARMAN_JOB_SERVERS` esté configurado correctamente.

2. **Trabajos no se procesan**: Verificar que los workers de Gearman estén ejecutándose y escuchando en los servidores configurados.

3. **Errores 400**: Revisar que todos los parámetros requeridos estén presentes y en el formato correcto.

---

## Versión

Esta documentación corresponde a la versión actual del código en `components-git-repo/dialer/interface`.

