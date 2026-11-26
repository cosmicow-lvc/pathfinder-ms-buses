package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const busColumns = `
	id::text, driver_id::text, patente, empresa, estado, updated_at, created_at
`

const driverColumns = `
	id::text, nombre, telefono, rut, activo
`

type rowScanner interface {
	Scan(...any) error
}

func scanBus(row rowScanner) (domain.Bus, error) {
	var bus domain.Bus
	err := row.Scan(
		&bus.ID,
		&bus.DriverID,
		&bus.Patente,
		&bus.Empresa,
		&bus.Estado,
		&bus.UpdatedAt,
		&bus.CreatedAt,
	)
	return bus, err
}

func scanDriver(row rowScanner) (domain.Driver, error) {
	var driver domain.Driver
	err := row.Scan(
		&driver.ID,
		&driver.Nombre,
		&driver.Telefono,
		&driver.RUT,
		&driver.Activo,
	)
	return driver, err
}

func (s *Store) CreateBus(ctx context.Context, bus domain.Bus) (domain.Bus, error) {
	query := `INSERT INTO buses (driver_id, patente, empresa, estado)
		VALUES ($1::uuid, $2, $3, $4)
		RETURNING ` + busColumns
	created, err := scanBus(s.pool.QueryRow(ctx, query, bus.DriverID, bus.Patente, bus.Empresa, bus.Estado))
	if err != nil {
		return domain.Bus{}, fmt.Errorf("create bus: %w", mapPostgresError(err))
	}
	return created, nil
}

func (s *Store) GetBus(ctx context.Context, id string) (domain.Bus, error) {
	query := `SELECT ` + busColumns + ` FROM buses WHERE id = $1::uuid`
	bus, err := scanBus(s.pool.QueryRow(ctx, query, id))
	if err != nil {
		return domain.Bus{}, fmt.Errorf("get bus: %w", mapPostgresError(err))
	}
	return bus, nil
}

func (s *Store) ListBuses(ctx context.Context) ([]domain.Bus, error) {
	query := `SELECT ` + busColumns + ` FROM buses ORDER BY created_at, id`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list buses: %w", err)
	}
	defer rows.Close()

	buses := make([]domain.Bus, 0)
	for rows.Next() {
		bus, err := scanBus(rows)
		if err != nil {
			return nil, fmt.Errorf("scan bus: %w", err)
		}
		buses = append(buses, bus)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate buses: %w", err)
	}
	return buses, nil
}

func (s *Store) UpdateBus(ctx context.Context, id string, bus domain.Bus) (domain.Bus, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Bus{}, fmt.Errorf("begin update bus transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var previousStatus string
	if err := tx.QueryRow(ctx, `SELECT estado FROM buses WHERE id = $1::uuid FOR UPDATE`, id).Scan(&previousStatus); err != nil {
		return domain.Bus{}, fmt.Errorf("get bus status for update: %w", mapPostgresError(err))
	}

	query := `UPDATE buses
		SET driver_id = $2::uuid, patente = $3, empresa = $4, estado = $5, updated_at = NOW()
		WHERE id = $1::uuid
		RETURNING ` + busColumns
	updated, err := scanBus(tx.QueryRow(ctx, query, id, bus.DriverID, bus.Patente, bus.Empresa, bus.Estado))
	if err != nil {
		return domain.Bus{}, fmt.Errorf("update bus: %w", mapPostgresError(err))
	}
	if previousStatus != updated.Estado {
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (evento, payload)
			VALUES (
				'bus.status.changed.v1',
				jsonb_build_object(
					'messageId', gen_random_uuid()::text,
					'type', 'bus.status.changed.v1',
					'occurredAt', transaction_timestamp(),
					'data', jsonb_build_object(
						'busId', $1::uuid,
						'previousEstado', $2::text,
						'estado', $3::text
					)
				)
			)`, id, previousStatus, updated.Estado)
		if err != nil {
			return domain.Bus{}, fmt.Errorf("record bus status change in outbox: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Bus{}, fmt.Errorf("commit bus update transaction: %w", err)
	}
	return updated, nil
}

func (s *Store) DeleteBus(ctx context.Context, id string) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM buses WHERE id = $1::uuid`, id)
	if err != nil {
		return fmt.Errorf("delete bus: %w", err)
	}
	if result.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *Store) BusPatenteExists(ctx context.Context, patente, exceptID string) (bool, error) {
	var exclude any
	if exceptID != "" {
		exclude = exceptID
	}
	const query = `SELECT EXISTS (
		SELECT 1 FROM buses
		WHERE patente = $1 AND ($2::uuid IS NULL OR id <> $2::uuid)
	)`
	var exists bool
	if err := s.pool.QueryRow(ctx, query, patente, exclude).Scan(&exists); err != nil {
		return false, fmt.Errorf("check bus patente: %w", err)
	}
	return exists, nil
}

func (s *Store) CreateDriver(ctx context.Context, driver domain.Driver) (domain.Driver, error) {
	query := `INSERT INTO driver (nombre, telefono, rut)
		VALUES ($1, $2, $3)
		RETURNING ` + driverColumns
	created, err := scanDriver(s.pool.QueryRow(ctx, query, driver.Nombre, driver.Telefono, driver.RUT))
	if err != nil {
		return domain.Driver{}, fmt.Errorf("create driver: %w", mapPostgresError(err))
	}
	return created, nil
}

func (s *Store) GetDriver(ctx context.Context, id string) (domain.Driver, error) {
	query := `SELECT ` + driverColumns + ` FROM driver WHERE id = $1::uuid`
	driver, err := scanDriver(s.pool.QueryRow(ctx, query, id))
	if err != nil {
		return domain.Driver{}, fmt.Errorf("get driver: %w", mapPostgresError(err))
	}
	return driver, nil
}

func (s *Store) ListDrivers(ctx context.Context) ([]domain.Driver, error) {
	query := `SELECT ` + driverColumns + ` FROM driver ORDER BY nombre, id`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list drivers: %w", err)
	}
	defer rows.Close()

	drivers := make([]domain.Driver, 0)
	for rows.Next() {
		driver, err := scanDriver(rows)
		if err != nil {
			return nil, fmt.Errorf("scan driver: %w", err)
		}
		drivers = append(drivers, driver)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate drivers: %w", err)
	}
	return drivers, nil
}

func (s *Store) UpdateDriver(ctx context.Context, id string, driver domain.Driver) (domain.Driver, error) {
	query := `UPDATE driver
		SET nombre = $2, telefono = $3, rut = $4, activo = $5
		WHERE id = $1::uuid
		RETURNING ` + driverColumns
	updated, err := scanDriver(s.pool.QueryRow(ctx, query, id, driver.Nombre, driver.Telefono, driver.RUT, driver.Activo))
	if err != nil {
		return domain.Driver{}, fmt.Errorf("update driver: %w", mapPostgresError(err))
	}
	return updated, nil
}

func (s *Store) DeactivateDriver(ctx context.Context, id string) (domain.Driver, error) {
	query := `UPDATE driver SET activo = FALSE
		WHERE id = $1::uuid
		RETURNING ` + driverColumns
	deactivated, err := scanDriver(s.pool.QueryRow(ctx, query, id))
	if err != nil {
		return domain.Driver{}, fmt.Errorf("deactivate driver: %w", mapPostgresError(err))
	}
	return deactivated, nil
}

func (s *Store) DriverRUTExists(ctx context.Context, rut, exceptID string) (bool, error) {
	var exclude any
	if exceptID != "" {
		exclude = exceptID
	}
	const query = `SELECT EXISTS (
		SELECT 1 FROM driver
		WHERE rut = $1 AND ($2::uuid IS NULL OR id <> $2::uuid)
	)`
	var exists bool
	if err := s.pool.QueryRow(ctx, query, rut, exclude).Scan(&exists); err != nil {
		return false, fmt.Errorf("check driver RUT: %w", err)
	}
	return exists, nil
}

func mapPostgresError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}

	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch {
		case postgresError.Code == "23505" && postgresError.ConstraintName == "buses_patente_key":
			return domain.ErrPatenteAlreadyExists
		case postgresError.Code == "23505" && postgresError.ConstraintName == "driver_rut_key":
			return domain.ErrRUTAlreadyExists
		case postgresError.Code == "23503" && postgresError.ConstraintName == "buses_driver_id_fkey":
			return domain.ErrDriverNotFound
		case postgresError.Code == "23514" && postgresError.ConstraintName == "buses_estado_check":
			return domain.ErrInvalidInput
		}
	}
	return err
}
