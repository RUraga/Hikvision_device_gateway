# hikvision-event-bridge

Servicio que corre junto al Hikvision Device Gateway, recibe eventos de control de acceso, los guarda en SQLite y los reenvía por HTTP a una o varias aplicaciones. No calcula jornadas, comidas, descansos ni horas trabajadas, y no identifica usuarios de Laravel. No abre las bases de datos de esas aplicaciones.

La confirmación de un destino significa que ese destino guardó el evento. El procesamiento laboral puede seguir pendiente.

## Flujo

1. El proceso abre `POST /ISAPI/Event/notification/subscribeDeviceMgmt?format=json` con autenticación Digest y lee el stream `multipart/mixed` sin cargarlo entero.
2. Cada `AccessControllerEvent` de un dispositivo habilitado que pasa los filtros se normaliza.
3. El evento y una entrega por destino se guardan en la misma transacción de SQLite, antes de cualquier POST.
4. Los trabajadores de entrega hacen `POST` con `Idempotency-Key`. Una entrega no mantiene abierta la transacción durante el HTTP.
5. Si el proceso se detiene a mitad de un envío, la entrega vuelve a `pending` al arrancar. No se da por entregada: el destino debe deduplicar con la misma clave.
6. La recuperación histórica, si está habilitada, consulta ventanas acotadas al iniciar, al reconectar y cada cierto intervalo. El avance por dispositivo solo se mueve cuando una ventana completa quedó guardada.

Con `eventMode: all` el gateway suscribe todos los dispositivos añadidos. El bridge filtra en local. La guía (sección 5.5.1) dice que en ese modo no se puede suscribir un solo dispositivo.

## Qué está cubierto por pruebas automáticas

La suite usa un gateway simulado y un servidor HTTP simulado. Cubre Digest, reconexión, multipart fragmentado, JSON inválido, límites de tamaño, normalización de tarjeta y rostro, conservación de `attendance_status` y `label`, persistencia previa al envío, lease vencido, timeout después de que el destino ya aceptó, duplicados, destinos independientes, 401, 403, 409, 422, 429 y 500, confirmación inválida, historial paginado, checkpoint que no avanza si una página falla, retención y apagado. El cliente HTTP también se prueba contra las respuestas exactas de Rancherito: 201 nuevo, 200 duplicado, `accepted: true` con cada `processing_status`, y los errores 401, 403, 409 y 422.

## Qué falta validar en el equipo

No está probado contra el Device Gateway ni contra el terminal de Rancherito. No está listo para producción hasta hacer, en el servidor real, una desconexión, una reconexión y una recuperación de eventos que el equipo todavía conserve.

Pendiente de contrastar con el hardware y con Laravel:

- La guía de ejemplo envía `Content-Type: application/x-www-form-urlencoded` aunque el cuerpo es JSON. Este servicio envía `application/json`.
- El catálogo numérico major/minor (apéndice C de la guía UD29329B) no venía en el texto recuperado. `result` queda en `unknown` hasta que se configuren códigos confirmados en el terminal. No se trata todo el major 5 como marcación válida.
- `serialNo` puede no coincidir entre el vivo y el historial. La guía (A.33, `frontSerialNo`) lo menciona como problema de la suscripción. La identidad provisional incluye el serial; si las dos fuentes discrepan, no se deduplican. Rancherito todavía impone `UNIQUE(device_uid, serial_no)`, así que hace falta la misma marcación capturada en vivo y en `AcsEvent` antes de dar el serial por estable.
- Rancherito traduce, solo en el reloj `9CE552AA-7ACD-7145-B340-A8618DA1B946`, `checkIn` a entrada, `checkOut` a salida, `breakIn` a inicio de descanso y `breakOut` a regreso. No exige `label`. Comida y descanso comparten esas opciones. El bridge reenvía `attendance_status` y `label` tal como llegan.
- `searchResultPosition` se envía como desplazamiento inicial (0, luego la suma de `numOfMatches`). Hay que confirmarlo con una consulta real.
- No se envía `picEnable`. La guía no aclara si filtra eventos o solo pide fotos.
- La ruta `POST https://admin.rancherito.com.mx/api/integrations/hikvision/events` ya está implementada. El bridge la llama con el token de `settings.key = hikvision_integration_token`.
- Un `NO MATCH` avanza el checkpoint. No distingue “no hubo eventos” de “el equipo ya no los conserva”.

Referencia usada: Hikvision Device Gateway API Developer Guide, UD29329B, V1.6.1 (2023-03-28). Secciones 2 (Digest), 5.4.9 (`AcsEvent`), 5.5 (suscripción y cancelación), A.2, A.3, A.33, A.114 y A.115.

## Requisitos

- Python 3.11 o superior.
- Red hasta el Device Gateway. En el servidor suele ser `http://127.0.0.1`.
- Un intérprete del sistema para crear el virtualenv. systemd usa el Python de ese virtualenv.

## Instalación local

Desde la raíz del repositorio, en PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item config.example.toml config.toml
Copy-Item .env.example .env
```

Edite `.env` o exporte las variables antes de arrancar. Si existe `.env` en el directorio actual, el comando lo carga sin sustituir variables que ya estén definidas. No imprime sus valores.

```powershell
$env:HIKVISION_GATEWAY_PASSWORD = "la-contraseña-del-gateway"
$env:RANCHERITO_EVENTS_TOKEN = "el-token-de-laravel"
.\.venv\Scripts\hikvision-event-bridge.exe check-config --config config.toml
python -m pytest
```

`config.toml` queda fuera de git. La base de ejemplo es `data/events.sqlite`, junto al archivo de configuración.

## Configuración inicial de Rancherito

`config.example.toml` deja:

- gateway `http://127.0.0.1`, usuario `admin`, secreto en `HIKVISION_GATEWAY_PASSWORD`;
- dispositivo `9CE552AA-7ACD-7145-B340-A8618DA1B946`, zona `America/Mexico_City`;
- destino `https://admin.rancherito.com.mx/api/integrations/hikvision/events`, secreto en `RANCHERITO_EVENTS_TOKEN`;
- filtro de autenticación `card` y `face`, según los valores documentados de `currentVerifyMode` (`card`, `face`, `cardOrFace` y el resto de combinaciones que contienen esos fragmentos);
- recuperación histórica activada con inicio explícito `2026-10-01T00:00:00-06:00`. Cámbielo antes de arrancar. No se importa el historial anterior a esa fecha y no se usa “ahora” como inicio silencioso.

Si el equipo envía `currentVerifyMode` vacío, esos eventos no salen con el filtro de ejemplo. Después de ver una muestra, se pueden añadir códigos minor confirmados en `authentication_sub_event_types`. El bridge no trae una lista predeterminada.

Para excluir puertas: `exclude_door_events = true` y `door_sub_event_types` con códigos vistos en el equipo. Si la lista está vacía, la configuración no arranca.

`success_sub_event_types` y `failure_sub_event_types` llenan `result` con `success` o `failure`. Si un código no está en ninguna, `result` es `unknown`.

## Variables de entorno

| Variable | Uso |
| --- | --- |
| `HIKVISION_GATEWAY_PASSWORD` | Contraseña Digest del gateway. El nombre se puede cambiar con `gateway.password_env`. |
| `RANCHERITO_EVENTS_TOKEN` | Token Bearer dedicado. En Rancherito está en `settings.key = hikvision_integration_token`. No es el token de impresión. |

No se guardan en TOML, SQLite ni en los logs habituales. `verify_tls` está activo por defecto. El cliente no usa variables de proxy del entorno, para no enviar el token a un proxy accidental.

## Ejecución

```powershell
.\.venv\Scripts\hikvision-event-bridge.exe run --config config.toml
.\.venv\Scripts\hikvision-event-bridge.exe run --config config.toml --dry-run --database data\dry-run.sqlite
.\.venv\Scripts\hikvision-event-bridge.exe status --config config.toml
.\.venv\Scripts\hikvision-event-bridge.exe retry-rejected --config config.toml --id 12
.\.venv\Scripts\hikvision-event-bridge.exe retry-rejected --config config.toml --event-id <event_id> --destination rancherito --include-conflict
.\.venv\Scripts\hikvision-event-bridge.exe backfill --config config.toml --device rancherito --start 2026-10-29T08:00:00-06:00 --end 2026-10-29T12:00:00-06:00
.\.venv\Scripts\hikvision-event-bridge.exe diagnostics --config config.toml
```

`status` abre la base en solo lectura. `run` impide una segunda instancia con un cerrojo y latido; si el proceso muere, el cerrojo se puede tomar cuando vence el TTL.

`--dry-run` no hace POST, no marca `delivered` y exige `--database` distinta de la de producción. Sí puede abrir la suscripción del gateway y escribir en esa otra base.

`backfill` consulta una ventana explícita. No salta el checkpoint por encima de un hueco: solo lo adelanta si la ventana conecta con el avance ya confirmado. Si el servicio ya está recuperando ese dispositivo, el comando se detiene en lugar de mezclar las páginas.

## Diagnóstico sin modificar dispositivos

`diagnostics` sin `--subscribe-sample` hace `GET /ISAPI/System/deviceInfo?format=json` y un saludo TCP/TLS al destino. No manda el token de Rancherito ni crea eventos.

`http://127.0.0.1` solo vale en el servidor del gateway o detrás de un túnel ya abierto. `https://hikvision.toucheim.com` responde Digest sobre TLS, detrás de nginx. El stream multipart todavía no está comprobado por ese dominio: no lo uses para la primera captura y no desactives `verify_tls`.

La captura espera un `AccessControllerEvent` del dispositivo configurado. No se detiene al tercer fragmento: una respuesta de suscripción o un heartbeat no cuentan. `--timeout` es el tope total, en segundos. El archivo se crea con permiso restrictivo. No hay POST a Rancherito ni se toca su cola.

En el servidor Linux del gateway, con `gateway.url = "http://127.0.0.1"`:

```bash
mkdir -p captures
chmod 700 captures
./.venv/bin/hikvision-event-bridge diagnostics --config config.toml --subscribe-sample --timeout 180 --capture captures/muestra.bin
```

En Windows, solo si `config.toml` apunta al túnel o a una URL cuyo stream ya se comprobó:

```powershell
New-Item -ItemType Directory -Force captures | Out-Null
.\.venv\Scripts\hikvision-event-bridge.exe diagnostics --config config.toml --subscribe-sample --timeout 180 --capture captures\muestra.bin
```

Mientras el comando espera, haz una sola marcación en el reloj configurado. Cuando imprima `marcación encontrada`, la suscripción se cancela.

Después, sin guardar la marcación en la base ni enviarla:

```bash
./.venv/bin/hikvision-event-bridge compare-sample --config config.toml --capture captures/muestra.bin --window-seconds 180
```

```powershell
.\.venv\Scripts\hikvision-event-bridge.exe compare-sample --config config.toml --capture captures\muestra.bin --window-seconds 180
```

Esa consulta pagina la ventana corta. No usa `maxResults=1`. Pide `major` 5 y `minor` 0, que en el equipo devolvieron la marcación junto con eventos de puerta; `minor` 0 no clasifica el resultado. Lee `AcsEvent.InfoList` y también `MatchList` si es la única lista. Si `numOfMatches` es mayor que cero y no hay lista legible, lo informa como error y no avanza el checkpoint. Compara dispositivo, serial, instante UTC, precisión de la fecha, `employee_no`, `card_no`, major/minor, `attendance_status`, `label` y el `event_id` de cada adaptador. Si un campo solo viene en una fuente, los identificadores pueden diferir. Una muestra no declara estable el serial ni cambia `UNIQUE(device_uid, serial_no)`.

Si hace falta un `curl` manual, la contraseña queda en el entorno y el cuerpo en un archivo:

```powershell
@'
{"AcsEventCond":{"searchID":"diag-1","searchResultPosition":0,"maxResults":30,"major":5,"minor":0,"startTime":"2026-10-29T08:55:00-06:00","endTime":"2026-10-29T09:05:00-06:00"}}
'@ | Set-Content -Encoding utf8 consulta.json
curl.exe --digest -u "admin:$env:HIKVISION_GATEWAY_PASSWORD" -X POST "http://127.0.0.1/ISAPI/AccessControl/AcsEvent?format=json&devIndex=9CE552AA-7ACD-7145-B340-A8618DA1B946" -H "Content-Type: application/json" --data-binary "@consulta.json"
```

En ese reloj, `checkIn`, `checkOut`, `breakIn` y `breakOut` ya están traducidos. Comida y descanso no se separan. Anota igual `label` y los tipos major/minor cuando lleguen, porque el mapa no los usa para decidir la acción.

## Rechazados

No se borran al agotar intentos.

- Red, tiempo de espera, HTTP 408, 429 y 5xx: siguen en `pending`, con espera exponencial, tope y variación aleatoria. `Retry-After` se respeta hasta `retry_after_cap_seconds`.
- 401 de Rancherito: token ausente o inválido. La entrega queda en `auth_blocked` y no se reintenta hasta `auth_hold_seconds`.
- 403 de Rancherito: el dispositivo no está registrado o está inactivo. También queda en `auth_blocked`. No se reescribe el evento ni se cambia su clave.
- Un 401 Digest del gateway, en la suscripción o en el historial, usa esa misma espera larga para no insistir sobre la cuenta del administrador.
- 400, 404, 413, 415, 422 y el resto de 4xx no listados como reintento: `rejected`. El 422 es un contrato inválido.
- 409: conflicto de identidad o de contenido. Queda en `conflict`. No se descarta ni se cambia el `event_id` para evitarlo.
- 3xx: no se sigue la redirección. Queda `rejected` con motivo `redirect_not_followed`.
- 200 o 201 con HTML, JSON inválido o `accepted` distinto de `true`: no se confirma. Sigue pendiente y se reintenta con la misma clave y la misma espera acotada.

`retry-rejected` solo reactiva las filas indicadas. `--include-conflict` hace falta para un 409.

Si se quita un destino de la configuración y quedan entregas abiertas, el servicio no arranca hasta declarar:

```toml
[orphaned_destinations.nombre-anterior]
action = "hold"
```

Esas filas pasan a `held`. No se mandan a otra URL.

Si el mismo destino cambia de URL, haga lo mismo con `pending_url_policy`: `hold`, `use_snapshot` (sigue en la URL guardada al crear la entrega) o `use_current` (adopta la URL nueva, decisión explícita). `use_snapshot` envía el token actual a la URL que quedó guardada.

## systemd

La unidad está en `deploy/hikvision-event-bridge.service`. No active el servicio desde aquí: hay que crear el usuario, copiar el virtualenv y escribir la configuración en el servidor.

```bash
sudo useradd --system --home /opt/hikvision-event-bridge --shell /usr/sbin/nologin hikvision-bridge
sudo install -d -o hikvision-bridge -g hikvision-bridge /opt/hikvision-event-bridge
sudo install -d -o root -g hikvision-bridge -m 0750 /etc/hikvision-event-bridge
# copiar el proyecto, crear el virtualenv e instalar con el mismo Python 3.11+
sudo install -m 0640 -o root -g hikvision-bridge config.toml /etc/hikvision-event-bridge/config.toml
sudo install -m 0640 -o root -g hikvision-bridge hikvision-event-bridge.env /etc/hikvision-event-bridge/hikvision-event-bridge.env
```

En el TOML de producción use:

```toml
sqlite_path = "/var/lib/hikvision-event-bridge/events.sqlite"
```

`StateDirectory=hikvision-event-bridge` crea ese directorio para el usuario del servicio. La unidad arranca después de `network-online.target`, reinicia si falla y carga los secretos con `EnvironmentFile`. El nombre de la unidad del Device Gateway todavía no está confirmado. Cuando exista, añada en `[Unit]`:

```ini
After=hikvision-device-gateway.service
Wants=hikvision-device-gateway.service
```

Si `MemoryDenyWriteExecute=true` impide arrancar el intérprete, quite solo esa línea y vuelva a cargar la unidad. No abre puertos ni cambia el firewall.

```bash
sudo cp deploy/hikvision-event-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hikvision-event-bridge.service
sudo systemctl status hikvision-event-bridge.service
```

## Copia de SQLite con el servicio activo

No copie el archivo `.sqlite` a mano mientras existan `-wal` y `-shm`. Desde el servidor:

```bash
sudo -u hikvision-bridge sqlite3 /var/lib/hikvision-event-bridge/events.sqlite ".backup /var/backups/hikvision-event-bridge.sqlite"
```

Esa copia es consistente con el servicio en marcha. La retención solo borra eventos cuyas entregas están todas en `delivered` y cuya entrega es más antigua que `retention.delivered_days`. Lo pendiente, rechazado, en conflicto o en espera no se borra por antigüedad.

## Datos personales y biometría

SQLite guarda el nombre, el número de empleado, la tarjeta y el JSON ya recortado. Proteja el archivo (`UMask=0077` en systemd).

Con `privacy.strip_biometric_fields = true` (valor por defecto) se quitan del JSON guardado y del POST las URL de imagen (`pictureURL`, luz visible, térmica y variantes), `filename`, coordenadas `RegionCoordinates`, temperatura facial y campos de plantilla. No se descargan fotografías. El número de empleado, la tarjeta, el nombre, la fecha, los tipos de evento, `attendanceStatus`, `label` y `currentVerifyMode` se conservan. Si hace falta auditar una URL, ponga el ajuste en `false` sabiendo que la base pasará a contener esos enlaces.

Los logs de operación llevan `event_id`, dispositivo, destino, código HTTP y contadores. No llevan contraseña, token, nombre ni tarjeta.

## Contrato con Rancherito

`POST https://admin.rancherito.com.mx/api/integrations/hikvision/events`

La ruta sigue siendo configurable. El token es el de `settings.key = hikvision_integration_token`, leído de `RANCHERITO_EVENTS_TOKEN`. No es el token de impresión.

Cabeceras:

```http
Authorization: Bearer <token dedicado>
Content-Type: application/json
Accept: application/json
Idempotency-Key: <el mismo event_id del cuerpo>
```

Rancherito acepta en ese header el `event_id` original o `sha256("hikvision-event-v1|" + event_id)`. El bridge envía siempre el `event_id` original. El hash es una clave de almacenamiento de Laravel y no sustituye la identidad del bridge. No cambia entre reintentos, vivo e historial.

Cuerpo, versión 1. `major_event_type` y `sub_event_type` son los del equipo; no se envían ceros en su lugar. `occurred_at` lleva offset o `Z`. `employee_no` y `card_no` son texto; si faltan se envía cadena vacía, no un número.

```json
{
  "schema_version": 1,
  "event_id": "<sha256 hex de la identidad provisional>",
  "device_index": "9CE552AA-7ACD-7145-B340-A8618DA1B946",
  "serial_no": 12345,
  "occurred_at": "2026-10-29T09:00:00-06:00",
  "employee_no": "0002",
  "card_no": "03322275427",
  "attendance_status": "checkIn",
  "label": "Entrada",
  "major_event_type": 5,
  "sub_event_type": 75,
  "result": "unknown",
  "verify_mode": "face",
  "name": "Ana",
  "source": "live",
  "raw": {}
}
```

Los números 5 y 75 de ese ejemplo no son un catálogo del terminal. En un evento real se reenvían los valores recibidos.

`result` es `success`, `failure` o `unknown`. Con las listas de códigos vacías sale `unknown`. Rancherito lo guarda como `pending_classification` y no abre ni cierra intervalos. Solo `success` entra al cálculo. `failure` se guarda y no modifica sesiones. No se interpreta `unknown` como éxito aunque haya `employee_no` y una acción reconocida. `verify_mode` es `currentVerifyMode`. `attendance_status` y `label` se reenvían tal como llegaron, hasta 32 y 100 caracteres. `employee_no` y `card_no` hasta 64. `name` hasta 100. `serial_no` es un entero de 0 a 4294967295. El bridge no traduce la acción: Rancherito lo hace con el `attendance_status` de ese reloj.

`source` es `live` o `backfill`. No forma parte de la identidad.

No se envía `user_id` de Laravel ni una acción laboral calculada.

`201` significa evento nuevo almacenado. `200` significa duplicado equivalente ya almacenado. En los dos casos la entrega se confirma solo si el JSON trae `accepted: true`.

```json
{
  "accepted": true,
  "duplicate": false,
  "event_id": 123,
  "processing_status": "pending"
}
```

`event_id` en la respuesta es el id interno de Laravel. No reemplaza al `event_id` enviado. `processing_status` puede ser `pending`, `processed`, `unresolved_identity`, `unmapped_action`, `ignored` o `error`. Si `accepted` es `true`, el bridge marca `delivered` aunque el cálculo laboral siga pendiente o haya terminado en error. Rancherito reprocesa lo ya recibido.

Errores, sin borrar el evento ni cambiar su clave:

| HTTP | Significado | Estado en el bridge |
| --- | --- | --- |
| 401 | Token ausente o inválido | `auth_blocked` |
| 403 | Dispositivo no registrado o inactivo | `auth_blocked` |
| 409 | Conflicto de identidad o contenido | `conflict` |
| 422 | Contrato inválido | `rejected` |

## Identidad provisional

Versión 1. SHA-256 de un JSON ordenado con `device_uid`, `serial_no`, la hora en UTC truncada al segundo, `employee_no`, `card_no`, `major_event_type` y `sub_event_type`.

No usa un UUID por recepción, ni `received_at`, ni `source`, ni el número de intento, ni el JSON completo. El vivo y el historial producen la misma clave cuando esos campos coinciden, aunque cambien de sitio (`dateTime` frente a `time`, `majorEventType` frente a `major`, `employeeNoString` frente a `employeeNo`).

Si la misma clave llega con otro nombre, etiqueta, estado de asistencia o modo de verificación, y los dos lados traen valor, se anota una incidencia y no se pisa el evento guardado. Un lado vacío no pisa al otro: el historial documentado puede omitir el nombre.

Limitación: si el serial del vivo y el del historial no coinciden, habrá dos eventos. Rancherito, además, todavía tiene `UNIQUE(device_uid, serial_no)`, así que ese desacuerdo también se ve allí. Si el equipo reutiliza un serial en el mismo segundo, con la misma persona, tarjeta y tipos, el bridge los trataría como el mismo evento. Hay que medirlo con una muestra en vivo y otra histórica de la misma marcación antes de considerar la clave definitiva. Un 409 no se resuelve cambiando el `event_id`.
