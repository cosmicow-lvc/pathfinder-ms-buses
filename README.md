# Pathfinder MS Buses

Microservicio Go responsable de la administración de buses y conductores.

## Contratos vigentes

Los estados publicados y aceptados siguen `schema.graphql`: `ACTIVO`,
`INACTIVO` y `EN_MANTENCION`. Se admite compatibilidad de entrada con los
nombres heredados en minúsculas. El evento vigente es `bus.status.changed` y
la migración `000004` normaliza los datos existentes.

El esquema define IDs UUID para `driver` y `buses`; cada bus requiere un
conductor, y un conductor puede estar asociado a varios buses. `outbox` es una
tabla independiente, sin claves foráneas hacia esas tablas.

La lógica de servicio ofrece operaciones CRUD para buses y conductores. Las
patentes se guardan en mayúsculas y los RUT se guardan en mayúsculas sin puntos
para evitar duplicados causados por diferencias de formato. Eliminar un
conductor lo desactiva (`activo = false`) para conservar sus datos.

## Kafka

El contrato completo esta documentado en
[docs/kafka-contracts.md](./docs/kafka-contracts.md). `ms-buses` consume tanto
los topics modernos (`buses.bus.*`, `buses.driver.*`, `bus.validate`) como los
canales heredados (`buses.commands.v1` y
`buses.validation.requests.v1`).

La validacion `bus.validate` responde por el `replyTo` autorizado y conserva el
`correlationId`. Solo considera disponible un bus existente con estado
`ACTIVO`. Cuando cambia el estado, la actualizacion y el evento
`bus.status.changed` se guardan en la misma transaccion. El publisher envia los
eventos pendientes al topic vigente, conserva compatibilidad con
`buses.events.v1` y reintenta ante fallos.

## Migraciones

Las migraciones PostgreSQL están en `migrations/` y se ejecutan con
[golang-migrate](https://github.com/golang-migrate/migrate).

Instala el CLI:

```powershell
go install -tags "postgres" github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

Define `POSTGRES_DSN` en la sesión actual de PowerShell y aplica las migraciones
desde la raíz del repositorio:

```powershell
$env:POSTGRES_DSN = "postgres://postgres:postgres@localhost:5432/pathfinder?sslmode=disable"
migrate -path migrations -database "$env:POSTGRES_DSN" up
```

Para consultar la versión aplicada:

```powershell
migrate -path migrations -database "$env:POSTGRES_DSN" version
```

Para revertir la última migración:

```powershell
migrate -path migrations -database "$env:POSTGRES_DSN" down 1
```

## Pruebas del repositorio

Hay dos flujos distintos:

1. **Tests y cobertura:** ejecuta Go tests y muestra el porcentaje total. No
   necesita levantar Buses ni Kafka; las pruebas de mensajería usan mocks.
2. **Prueba manual de integración:** levanta Buses, PostgreSQL y Kafka para
   enviar mensajes reales y revisar respuestas y eventos.

No se necesitan el BFF, Rutas ni el repositorio de infraestructura para ninguno
de estos flujos. Docker Desktop debe estar iniciado.

### 1. Ejecutar tests y ver cobertura

Desde la raíz del repositorio, ejecuta:

```bash
docker compose -f compose.test.yml --profile test run --rm go-test
```

No necesitas instalar Go ni `migrate` en tu equipo. Compose prepara la base
aislada `pathfinder_test`, aplica las migraciones y ejecuta `go test` con
cobertura. Al final muestra el porcentaje total y también el porcentaje por
paquete. El perfil usa PostgreSQL, pero no inicia Buses ni Kafka.

El detalle queda en `coverage.out` en la raíz del repositorio; está ignorado
por Git. Si tienes levantado el entorno completo y quieres liberar recursos
antes de medir cobertura, detenlo primero con `docker compose -f compose.test.yml down`.

### 2. Probar Buses manualmente con Kafka

Levanta el entorno completo desde PowerShell:

```powershell
docker compose -f compose.test.yml up --build -d
docker compose -f compose.test.yml ps
Invoke-RestMethod http://localhost:8080/healthz
Invoke-RestMethod http://localhost:8080/readyz
```

Este Compose levanta Buses, su PostgreSQL y un broker Kafka local. Aplica las
migraciones de la base `pathfinder` y crea los topics necesarios. Kafka queda
disponible desde Windows en `localhost:9092`; los contenedores se comunican con
`kafka:29092`.

Abre consumidores en terminales PowerShell separadas. Déjalos abiertos mientras
envías mensajes; detenlos con `Ctrl+C`:

```powershell
docker compose -f compose.test.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic bff.replies.v1 --from-beginning --property print.key=true
```

```powershell
docker compose -f compose.test.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic routes.replies.v1 --from-beginning --property print.key=true
```

```powershell
docker compose -f compose.test.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic bus.status.changed --from-beginning --property print.key=true
```

Desde otra terminal, crea un conductor en `buses.commands.v1`. La respuesta
debe aparecer en `bff.replies.v1` con el mismo `correlationId`:

```powershell
@'
{"messageId":"11111111-1111-4111-8111-111111111111","correlationId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","type":"driver.create.v1","replyTo":"bff.replies.v1","sentAt":"2026-11-25T18:00:00Z","data":{"nombre":"Conductor Prueba","telefono":"+56912345678","rut":"12345678-5"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.commands.v1
```

Copia el `driverId` de la respuesta y úsalo para crear un bus. Reemplaza el
UUID de ejemplo por el ID real:

```powershell
@'
{"messageId":"22222222-2222-4222-8222-222222222222","correlationId":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","type":"bus.create.v1","replyTo":"bff.replies.v1","sentAt":"2026-11-25T18:01:00Z","data":{"driverId":"123e4567-e89b-12d3-a456-426614174000","patente":"ABCD12","empresa":"Empresa Prueba","estado":"activo"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.commands.v1
```

Copia el `busId` de la respuesta de creación y úsalo para validar el bus desde
`buses.validation.requests.v1`. Reemplaza el ID de ejemplo:

```powershell
@'
{"messageId":"33333333-3333-4333-8333-333333333333","correlationId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","type":"bus.validate.v1","replyTo":"routes.replies.v1","sentAt":"2026-11-25T18:02:00Z","data":{"busId":"REEMPLAZAR_POR_BUS_ID"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.validation.requests.v1
```

La respuesta en `routes.replies.v1` debe conservar el `correlationId` y tener
`valid: true`. Luego actualiza el estado del bus. Reemplaza ambos IDs por los
valores devueltos en las respuestas anteriores:

```powershell
@'
{"messageId":"44444444-4444-4444-8444-444444444444","correlationId":"dddddddd-dddd-4ddd-8ddd-dddddddddddd","type":"bus.update.v1","replyTo":"bff.replies.v1","sentAt":"2026-11-25T18:03:00Z","data":{"busId":"REEMPLAZAR_POR_BUS_ID","driverId":"REEMPLAZAR_POR_DRIVER_ID","patente":"ABCD12","empresa":"Empresa Prueba","estado":"mantenimiento"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.commands.v1
```

La respuesta del cambio llega a `bff.replies.v1` y el evento
`bus.status.changed` a `bus.status.changed`. Vuelve a publicar
`bus.validate.v1` con nuevos `messageId` y `correlationId`: debe responder
`valid: false`.

Para completar las operaciones CRUD, envía `bus.get.v1`, `bus.list.v1`,
`bus.delete.v1`, `driver.get.v1`, `driver.list.v1`, `driver.update.v1` y
`driver.deactivate.v1` a `buses.commands.v1`. Los campos de cada mensaje están
en [docs/kafka-contracts.md](./docs/kafka-contracts.md).

Los IDs y el RUT de los ejemplos son de muestra. Para repetir la prueba sin
borrar la base, usa otro RUT y otra patente para evitar duplicados.

### Detener o reiniciar el entorno

Detén los contenedores conservando los datos PostgreSQL:

```powershell
docker compose -f compose.test.yml down
```

Kafka no tiene volumen persistente: al detener el Compose se pierden sus
mensajes. Para borrar también los datos PostgreSQL y comenzar desde cero:

```powershell
docker compose -f compose.test.yml down -v
```

## Imagen Docker

Con Docker Desktop usando contenedores Linux, construye la imagen desde la raíz
del repositorio:

```powershell
docker build -t pathfinder-ms-buses:local .
```

La imagen contiene solo el binario de Buses y ejecuta como usuario no root.
PostgreSQL y Kafka deben ser accesibles desde el contenedor; `localhost` dentro
del contenedor apunta al propio contenedor, no al host ni a otros servicios.
