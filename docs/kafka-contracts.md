# Contratos Kafka de Buses

Este contrato define la comunicacion de Buses con el BFF y Rutas. Los nombres
de los topicos incluyen `v1`; cualquier cambio incompatible requiere una nueva
version. Los mensajes JSON usan `camelCase`, IDs UUID y fechas RFC 3339 en UTC.

## Topicos y direccion

| Topico | Productor | Consumidor | Uso |
| --- | --- | --- | --- |
| `buses.commands.v1` | BFF | Buses | Operaciones CRUD de buses y conductores |
| `buses.validation.requests.v1` | Rutas | Buses | Validar un bus antes de usarlo en un viaje |
| `buses.events.v1` | Buses | BFF y Rutas | Eventos de dominio publicados desde outbox |

Cada solicitud incluye `replyTo`, que indica el topico de respuesta del
solicitante. Como ejemplos, el BFF puede usar `bff.replies.v1` y Rutas
`routes.replies.v1`. Buses solo debe publicar en topicos de respuesta
permitidos por configuracion.

Los consumidores deben usar grupos independientes para comandos del BFF y
validaciones de Rutas, por ejemplo `pathfinder-buses-bff-v1` y
`pathfinder-buses-routes-v1`.

## Solicitudes y respuestas

Las solicitudes usan JSON UTF-8 y este sobre:

```json
{
  "messageId": "uuid",
  "correlationId": "uuid",
  "type": "bus.validate.v1",
  "replyTo": "routes.replies.v1",
  "sentAt": "2026-11-25T18:00:00Z",
  "data": {
    "busId": "uuid"
  }
}
```

- `messageId` identifica unicamente la solicitud y sirve para deduplicarla.
- `correlationId` se copia sin cambios en la respuesta.
- `type` identifica la operacion.
- `replyTo` es el topico de respuesta controlado por el solicitante.
- `sentAt` es una fecha RFC 3339 en UTC.
- La clave Kafka es el ID del bus o conductor cuando existe; en operaciones
  de creacion se usa `correlationId`.
- Las respuestas usan `correlationId` como clave Kafka.

La respuesta usa el mismo `correlationId`, un nuevo `messageId`, un tipo
`<operacion>.result.v1` y uno de estos formatos:

```json
{
  "messageId": "uuid",
  "correlationId": "uuid",
  "type": "bus.validate.result.v1",
  "sentAt": "2026-11-25T18:00:01Z",
  "data": {
    "busId": "uuid",
    "valid": true,
    "estado": "activo",
    "reason": null
  },
  "error": null
}
```

```json
{
  "messageId": "uuid",
  "correlationId": "uuid",
  "type": "bus.get.result.v1",
  "sentAt": "2026-11-25T18:00:01Z",
  "data": null,
  "error": {
    "code": "NOT_FOUND",
    "message": "Bus no encontrado"
  }
}
```

`code` es estable y debe usarse para tomar decisiones; `message` es solo
descriptivo. Codigos comunes: `INVALID_REQUEST`, `NOT_FOUND`,
`PATENTE_ALREADY_EXISTS`, `RUT_ALREADY_EXISTS`, `DRIVER_NOT_FOUND` e
`INTERNAL_ERROR`. Un tipo de operacion desconocido responde con
`UNSUPPORTED_OPERATION`.

## Operaciones

En `buses.commands.v1`, `type` puede ser:

| Tipo | Campos de `data` |
| --- | --- |
| `bus.create.v1` | `driverId`, `patente`, `empresa`, `estado` |
| `bus.get.v1` | `busId` |
| `bus.list.v1` | Objeto vacio |
| `bus.update.v1` | `busId`, `driverId`, `patente`, `empresa`, `estado` |
| `bus.delete.v1` | `busId` |
| `driver.create.v1` | `nombre`, `telefono`, `rut` |
| `driver.get.v1` | `driverId` |
| `driver.list.v1` | Objeto vacio |
| `driver.update.v1` | `driverId`, `nombre`, `telefono`, `rut`, `activo` |
| `driver.deactivate.v1` | `driverId` |

Los resultados exitosos devuelven el recurso en `data`; las listas usan
`data.items`. Desactivar un conductor conserva el registro y cambia `activo`
a `false`.

En `buses.validation.requests.v1`, Rutas envia `bus.validate.v1` con
`data.busId`. La respuesta `bus.validate.result.v1` expresa el resultado de
negocio en `data`: `valid` es `true` solo si el bus existe y su estado es
`activo`. Si no existe o no esta activo, `valid` es `false` y `reason` es
`BUS_NOT_FOUND` o `BUS_NOT_ACTIVE`; estos casos no son errores de transporte.
Un mensaje malformado que tenga `correlationId` y `replyTo` usa
`error.code = INVALID_REQUEST`.

## Estados y eventos

Los valores permitidos para `estado` son `activo`, `inactivo` y
`mantenimiento`. Solo `activo` permite validar un bus para un viaje.

Cuando cambia el estado de un bus, Buses publica en `buses.events.v1`:

```json
{
  "messageId": "uuid",
  "type": "bus.status.changed.v1",
  "occurredAt": "2026-11-25T18:00:00Z",
  "data": {
    "busId": "uuid",
    "previousEstado": "activo",
    "estado": "mantenimiento"
  }
}
```

La clave Kafka del evento es `busId` para conservar el orden de eventos del
mismo bus. No se publica el evento si el estado no cambia. El evento se guarda
en outbox en la misma transaccion que la actualizacion del bus.

## Entrega y errores

Los consumidores confirman el offset despues de procesar la solicitud y
publicar su respuesta. Un reintento puede volver a entregar una solicitud; el
`messageId` debe usarse para evitar repetir efectos. Los eventos tambien deben
ser idempotentes para sus consumidores.
