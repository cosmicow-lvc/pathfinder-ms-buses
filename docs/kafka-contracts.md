# Contratos Kafka de `ms-buses`

Este documento describe la comunicacion Kafka implementada y probada por
`ms-buses` con `bff-gateway` y `ms-rutas`.

Las reglas comunes son:

- JSON UTF-8 con campos `camelCase`.
- IDs representados como strings UUID.
- Fechas ISO 8601 / RFC 3339 en UTC.
- Estados de bus contractuales: `ACTIVO`, `INACTIVO` y `EN_MANTENCION`.
- La clave Kafka de un evento es el `busId`.
- Las respuestas conservan el `correlationId` recibido.
- La entrega es al menos una vez: consumidores y procesadores deben ser
  idempotentes.

## Mapa de topics

| Topic | Sentido | Uso |
| --- | --- | --- |
| `buses.commands.v1` | BFF -> Buses | Canal heredado con `type` en el payload |
| `buses.driver.list` | BFF -> Buses | Listar conductores |
| `buses.driver.create` | BFF -> Buses | Crear conductor |
| `buses.bus.list` | BFF -> Buses | Listar o filtrar buses |
| `buses.bus.get` | BFF -> Buses | Obtener un bus |
| `buses.bus.create` | BFF -> Buses | Crear bus |
| `buses.bus.change-status` | BFF -> Buses | Cambiar estado de un bus |
| `buses.validation.requests.v1` | Rutas -> Buses | Canal heredado de validacion |
| `bus.validate` | Rutas -> Buses | Validar si un bus existe y esta disponible |
| `bus.status.changed` | Buses -> Rutas/BFF | Evento de dominio publicado desde outbox |
| `buses.events.v1` | Buses -> Rutas/BFF | Topic heredado de eventos |
| `bff.replies.v1` | Buses -> BFF | Respuesta heredada al BFF |
| `routes.replies.v1` | Buses -> Rutas | Respuesta heredada a Rutas |
| `bus.validate.reply` | Buses -> Rutas | Respuesta del canal moderno de validacion |

El servicio consume los canales modernos y heredados con grupos independientes:

- `pathfinder-buses-bff-v1`: comandos del BFF.
- `pathfinder-buses-routes-v1`: validaciones de Rutas.

Cada grupo puede consumir varios topics. Los cambios incompatibles requieren
un topic nuevo o una version distinta.

## Headers de correlacion

Toda operacion debe llevar estos identificadores:

| Header | Uso |
| --- | --- |
| `x-correlation-id` | Trazabilidad punta a punta |
| `kafka_correlationId` | Correlacion request-reply |
| `kafka_replyTopic` | Topic de respuesta |

Las solicitudes modernas pueden transportar `correlationId` y `replyTo` en el
payload. Si faltan, el handler puede leerlos desde
`kafka_correlationId` / `kafka_replyTopic` y `x-correlation-id`.

Si no llega `correlationId`, `ms-buses` genera uno nuevo para la respuesta.
El BFF debe devolver siempre el id recibido al frontend.

## Envelope de solicitud

El formato vigente es un envelope JSON por request-reply:

```json
{
  "messageId": "3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
  "correlationId": "f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
  "type": "bus.validate",
  "replyTo": "bus.validate.reply",
  "sentAt": "2026-11-26T18:12:00Z",
  "data": {
    "busId": "2920a680-37f8-449b-9c50-719cf53789e2"
  }
}
```

- `messageId`: identifica la solicitud y permite deduplicar efectos.
- `correlationId`: identifica el recorrido completo de la operacion.
- `type`: identifica la operacion en el canal heredado.
- `replyTo`: topic autorizado donde debe publicarse la respuesta.
- `sentAt`: fecha de emision en formato ISO 8601.
- `data`: payload propio de la operacion.

En los topics modernos dedicados no es obligatorio enviar `type`; el handler
lo obtiene del topic que recibio. Deben enviarse `messageId`,
`correlationId`, `replyTo` y `data`.

## Envelope de respuesta

Todas las respuestas exitosas y fallidas siguen esta forma:

```json
{
  "messageId": "684e8fa9-d865-4646-8cd0-eb0350ff6afd",
  "correlationId": "f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
  "type": "bus.validate.result.v1",
  "sentAt": "2026-11-26T18:12:49Z",
  "ok": true,
  "data": {},
  "error": null
}
```

Respuesta con error:

```json
{
  "messageId": "b651bf11-b30a-4d5d-aa2f-e23e034f6790",
  "correlationId": "f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
  "type": "bus.get.result.v1",
  "sentAt": "2026-11-26T18:13:00Z",
  "ok": false,
  "data": null,
  "error": {
    "code": "NOT_FOUND",
    "message": "Recurso no encontrado"
  }
}
```

`ok` indica el resultado de la operacion; `code` es estable y debe usarse para
tomar decisiones; `message` solo describe el problema al frontend.

## Operaciones modernas

### Conductores

| Topic | Request `data` | Respuesta exitosa |
| --- | --- | --- |
| `buses.driver.list` | `{}` | `Driver[]` |
| `buses.driver.create` | `{ nombre, telefono, rut }` | `Driver` |

### Buses

| Topic | Request `data` | Respuesta exitosa |
| --- | --- | --- |
| `buses.bus.list` | `{ estado?, ids? }` | `Bus[]` |
| `buses.bus.get` | `{ id }` o `{ busId }` | `Bus` |
| `buses.bus.create` | `{ patente, empresa, driverId }` | `Bus` |
| `buses.bus.change-status` | `{ id, estado }` o `{ busId, estado }` | `Bus` |

Ejemplo de lista:

```json
{
  "estado": "ACTIVO",
  "ids": [
    "2920a680-37f8-449b-9c50-719cf53789e2"
  ]
}
```

## Canal heredado `buses.commands.v1`

Este canal sigue disponible para clientes existentes. El topic contiene
mensajes con `type` en el envelope.

| `type` | Campos de `data` |
| --- | --- |
| `bus.create.v1` | `driverId`, `patente`, `empresa`, `estado` |
| `bus.get.v1` | `busId` |
| `bus.list.v1` | `{}` |
| `bus.update.v1` | `busId`, `driverId`, `patente`, `empresa`, `estado` |
| `bus.delete.v1` | `busId` |
| `driver.create.v1` | `nombre`, `telefono`, `rut` |
| `driver.get.v1` | `driverId` |
| `driver.list.v1` | `{}` |
| `driver.update.v1` | `driverId`, `nombre`, `telefono`, `rut`, `activo` |
| `driver.deactivate.v1` | `driverId` |

Las listas heredadas usan `data.items`. Las respuestas modernas devuelven el
array directamente en `data`.

## Validacion de bus

Rutas envía al topic `bus.validate`:

```json
{
  "messageId": "33333333-3333-4333-8333-333333333333",
  "correlationId": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "replyTo": "bus.validate.reply",
  "data": {
    "busId": "2920a680-37f8-449b-9c50-719cf53789e2"
  }
}
```

Resultado:

```json
{
  "messageId": "684e8fa9-d865-4646-8cd0-eb0350ff6afd",
  "correlationId": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "type": "bus.validate.result.v1",
  "sentAt": "2026-11-26T18:12:49Z",
  "ok": true,
  "data": {
    "busId": "2920a680-37f8-449b-9c50-719cf53789e2",
    "valid": true,
    "exists": true,
    "available": true,
    "estado": "ACTIVO",
    "reason": null
  },
  "error": null
}
```

Reglas:

- `valid` es `true` solo si el bus existe y su estado es `ACTIVO`.
- Si el bus no existe:
  `valid=false`, `exists=false`, `available=false`, `reason=BUS_NOT_FOUND`.
- Si existe pero no esta activo:
  `valid=false`, `exists=true`, `available=false`, `reason=BUS_NOT_ACTIVE`.
- Estos casos son resultados de negocio, no errores de transporte.

El canal heredado `buses.validation.requests.v1` con
`type="bus.validate.v1"` conserva el mismo formato de `data`.

## Estados permitidos

| Estado interno/contrato | Valido para viajes |
| --- | --- |
| `ACTIVO` | Si |
| `INACTIVO` | No |
| `EN_MANTENCION` | No |

La migracion `000004_align_bus_status_contract` convierte los nombres
heredados existentes:

- `activo` -> `ACTIVO`
- `inactivo` -> `INACTIVO`
- `mantenimiento` -> `EN_MANTENCION`

`ms-buses` tambien acepta estas variantes al recibir datos. Las respuestas y
eventos siempre usan los nombres contractuales actuales.

## Evento `bus.status.changed`

El evento se publica por outbox cuando una actualizacion cambia efectivamente
el estado del bus:

```json
{
  "messageId": "f1d4b5a0-3461-4d50-afe7-0e70a8058db1",
  "eventId": "952007e1-d645-4703-9b86-65855268d365",
  "type": "bus.status.changed.v1",
  "eventType": "bus.status.changed",
  "occurredAt": "2026-11-26T18:21:52Z",
  "correlationId": "e3a1c9d0-5b7e-4c1a-9d2f-0a6b8c4e1f33",
  "data": {
    "busId": "2920a680-37f8-449b-9c50-719cf53789e2",
    "previousEstado": "EN_MANTENCION",
    "estado": "ACTIVO",
    "estadoAnterior": "EN_MANTENCION",
    "estadoNuevo": "ACTIVO"
  }
}
```

Topics configurados:

- Vigente: `bus.status.changed`.
- Heredado: `buses.events.v1`.

La clave Kafka es `busId`. Si el estado no cambia, no se genera evento.

## Outbox y entrega

La actualizacion del bus y el registro del evento se realizan dentro de la
misma transaccion.

El publisher:

1. Recupera el siguiente evento pendiente.
2. Lo publica en Kafka usando el `busId` como clave.
3. Marca la fila como publicada solo despues de recibir confirmacion.
4. Reintenta con backoff exponencial ante fallos.

Si el proceso se detiene despues de publicar pero antes de marcar el registro,
Kafka puede entregar el evento otra vez. Los consumidores deben deduplicar
usando `eventId`; si no usan almacenamiento dedicado, deben aplicar operaciones
idempotentes.

## Codigos de error

| Codigo | Cuando ocurre |
| --- | --- |
| `BAD_REQUEST` | Datos invalidos en el canal moderno |
| `INVALID_REQUEST` | Solicitud invalida en el canal heredado |
| `NOT_FOUND` | Bus, conductor o recurso inexistente |
| `DRIVER_NOT_FOUND` | El conductor referenciado no existe |
| `CONFLICT` | Duplicado en el canal moderno |
| `PATENTE_ALREADY_EXISTS` | Patente duplicada en el canal heredado |
| `RUT_ALREADY_EXISTS` | RUT duplicado en el canal heredado |
| `BUS_NOT_FOUND` | El bus validado no existe |
| `BUS_NOT_ACTIVE` | El bus existe pero no esta disponible |
| `UNSUPPORTED_OPERATION` | El `type` no esta soportado |
| `INTERNAL` | Error inesperado en el canal moderno |
| `INTERNAL_ERROR` | Error inesperado en el canal heredado |

Las respuestas de error deben contener mensajes comprensibles en espanol,
sin detalles internos, stack traces ni SQL.

## Reglas de consumo

1. Confirmar el offset solo despues de procesar y publicar la respuesta.
2. Ante JSON malformado, registrar el error y continuar con el siguiente mensaje.
3. Rechazar `replyTo` que no pertenezca a la lista configurada.
4. Usar `messageId` para deduplicar solicitudes.
5. Usar `eventId` para deduplicar eventos.
6. Propagar el mismo `correlationId` a respuestas, eventos y logs.
7. Generar uno nuevo solo si la solicitud no incluye trazabilidad.
