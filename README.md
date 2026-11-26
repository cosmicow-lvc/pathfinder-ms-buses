# Pathfinder MS Buses

Microservicio Go responsable de la administración de buses y conductores.

El esquema define IDs UUID para `driver` y `buses`; cada bus requiere un
conductor, y un conductor puede estar asociado a varios buses. `outbox` es una
tabla independiente, sin claves foráneas hacia esas tablas.

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

## Imagen Docker

Con Docker Desktop usando contenedores Linux, construye la imagen desde la raíz
del repositorio:

```powershell
docker build -t pathfinder-ms-buses:local .
```

La imagen contiene solo el binario de Buses y ejecuta como usuario no root.
PostgreSQL y Kafka deben ser accesibles desde el contenedor; `localhost` dentro
del contenedor apunta al propio contenedor, no al host ni a otros servicios.
