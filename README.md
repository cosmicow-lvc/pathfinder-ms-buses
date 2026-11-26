# Pathfinder MS Buses

Microservicio Go responsable de la administración de buses y conductores.

El esquema define IDs UUID para `driver` y `buses`; cada bus requiere un
conductor, y un conductor puede estar asociado a varios buses. `outbox` es una
tabla independiente, sin claves foráneas hacia esas tablas.

La lógica de servicio ofrece operaciones CRUD para buses y conductores. Las
patentes se guardan en mayúsculas y los RUT se guardan en mayúsculas sin puntos
para evitar duplicados causados por diferencias de formato. Eliminar un
conductor lo desactiva (`activo = false`) para conservar sus datos.

## Kafka

El contrato de mensajes, topicos y respuestas esta documentado en
[docs/kafka-contracts.md](./docs/kafka-contracts.md). Los consumidores de
comandos del BFF y validaciones de Rutas estan conectados. La validacion
`bus.validate.v1` responde por el `replyTo` permitido, conserva el
`correlationId` y solo considera valido un bus existente con estado `activo`.
Cuando cambia el estado de un bus, la actualizacion y el evento
`bus.status.changed.v1` se guardan en la misma transaccion. Un publisher en
goroutine envia los eventos pendientes a `buses.events.v1` y reintenta ante
fallos.

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

## Probar el backend

### Pruebas automáticas y cobertura

Las pruebas unitarias no necesitan servicios externos. La prueba de
`internal/store` sí necesita una base PostgreSQL con todas las migraciones
aplicadas; si `POSTGRES_TEST_DSN` no está definido, esa prueba se omite.

En PowerShell:

```powershell
$testDsn = "postgres://postgres:postgres@localhost:5432/pathfinder_test?sslmode=disable"
$env:POSTGRES_DSN = $testDsn
$env:POSTGRES_TEST_DSN = $testDsn
migrate -path migrations -database "$env:POSTGRES_DSN" up
go test ./...
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | Select-Object -Last 1
```

En Bash:

```bash
export POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/pathfinder_test?sslmode=disable"
export POSTGRES_TEST_DSN="$POSTGRES_DSN"
migrate -path migrations -database "$POSTGRES_DSN" up
go test ./...
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | tail -n 1
```

Usa una base de pruebas separada, porque la prueba crea y elimina registros.

### Entorno local aislado con PostgreSQL y Kafka

`compose.test.yml` levanta solo PostgreSQL, Kafka y Buses, y aplica las
migraciones automáticamente. No requiere el BFF, Rutas ni el repositorio de
infraestructura. Docker Desktop debe estar iniciado.

Este archivo es un entorno de desarrollo, no una unidad de despliegue: Buses
se compila como una imagen independiente y solo necesita su propia base de
datos y acceso a Kafka. El broker de un nodo permite probar el flujo, pero no
simula alta disponibilidad, tolerancia a fallos ni escalado de producción.

Desde PowerShell, en la raíz del repositorio:

```powershell
docker compose -f compose.test.yml up --build -d
docker compose -f compose.test.yml ps
Invoke-RestMethod http://localhost:8080/healthz
Invoke-RestMethod http://localhost:8080/readyz
```

Kafka queda disponible desde Windows en `localhost:9092`; los contenedores se
comunican con `kafka:29092`. Los topics de comandos, validación, eventos y
respuestas se crean al iniciar.

Abre consumidores en terminales PowerShell separadas para ver las respuestas y
los eventos:

```powershell
docker compose -f compose.test.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic bff.replies.v1 --from-beginning --property print.key=true
```

```powershell
docker compose -f compose.test.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic routes.replies.v1 --from-beginning --property print.key=true
```

```powershell
docker compose -f compose.test.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic buses.events.v1 --from-beginning --property print.key=true
```

Envía mensajes desde otra terminal. Este ejemplo crea un conductor; la respuesta
en `bff.replies.v1` debe conservar el `correlationId` y devolver el conductor
creado:

```powershell
@'
{"messageId":"11111111-1111-4111-8111-111111111111","correlationId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","type":"driver.create.v1","replyTo":"bff.replies.v1","sentAt":"2026-11-25T18:00:00Z","data":{"nombre":"Conductor Prueba","telefono":"+56912345678","rut":"12345678-5"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.commands.v1
```

Usa el `driverId` de la respuesta para crear un bus. Reemplaza el UUID de
ejemplo por el valor real:

```powershell
@'
{"messageId":"22222222-2222-4222-8222-222222222222","correlationId":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","type":"bus.create.v1","replyTo":"bff.replies.v1","sentAt":"2026-11-25T18:01:00Z","data":{"driverId":"123e4567-e89b-12d3-a456-426614174000","patente":"ABCD12","empresa":"Empresa Prueba","estado":"activo"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.commands.v1
```

Para verificar el CRUD, envía `bus.get.v1`, `bus.list.v1`,
`bus.update.v1` y `bus.delete.v1` a `buses.commands.v1`; para conductores,
usa los tipos `driver.get.v1`, `driver.list.v1`, `driver.update.v1` y
`driver.deactivate.v1`. Los campos requeridos están en
[docs/kafka-contracts.md](./docs/kafka-contracts.md).

Prueba la validación de Rutas usando el `busId` de la respuesta de creación:

```powershell
@'
{"messageId":"33333333-3333-4333-8333-333333333333","correlationId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","type":"bus.validate.v1","replyTo":"routes.replies.v1","sentAt":"2026-11-25T18:02:00Z","data":{"busId":"123e4567-e89b-12d3-a456-426614174001"}}
'@ | docker compose -f compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic buses.validation.requests.v1
```

La respuesta debe llegar a `routes.replies.v1` con el mismo `correlationId` y
`valid: true`. Cambia el estado a `mantenimiento` con `bus.update.v1`: debe
publicarse `bus.status.changed.v1` en `buses.events.v1` y la siguiente
validación debe indicar `valid: false`.

Los mensajes de respuesta y evento permanecen en Kafka mientras el entorno
está levantado. Para detenerlo conservando los datos PostgreSQL:

```powershell
docker compose -f compose.test.yml down
```

Para borrar también los datos de prueba PostgreSQL y comenzar desde cero:

```powershell
docker compose -f compose.test.yml down -v
```

No ejecutes las pruebas de integración que usan `POSTGRES_TEST_DSN` contra la
misma base mientras Buses está procesando mensajes; usa una base de pruebas
separada, como se indica arriba.

Ejemplo mínimo para validar un ID inexistente desde una herramienta Kafka:

```json
{
  "messageId": "3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
  "correlationId": "f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
  "type": "bus.validate.v1",
  "replyTo": "routes.replies.v1",
  "sentAt": "2026-11-25T18:00:00Z",
  "data": {
    "busId": "123e4567-e89b-12d3-a456-426614174000"
  }
}
```

La respuesta debe llegar a `routes.replies.v1` con el mismo `correlationId`,
`valid: false` y `reason: "BUS_NOT_FOUND"`.

## Imagen Docker

Con Docker Desktop usando contenedores Linux, construye la imagen desde la raíz
del repositorio:

```powershell
docker build -t pathfinder-ms-buses:local .
```

La imagen contiene solo el binario de Buses y ejecuta como usuario no root.
PostgreSQL y Kafka deben ser accesibles desde el contenedor; `localhost` dentro
del contenedor apunta al propio contenedor, no al host ni a otros servicios.
