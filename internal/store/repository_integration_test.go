package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
)

func TestPostgresRepositoryCRUD(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	repo, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	rut := "TEST-" + suffix
	patente := "TEST-" + suffix

	t.Cleanup(func() {
		if _, err := repo.pool.Exec(context.Background(), `
			DELETE FROM buses WHERE patente = $1 OR patente = $2`,
			patente, patente+"-INVALID"); err != nil {
			t.Errorf("clean up test bus: %v", err)
		}
		if _, err := repo.pool.Exec(context.Background(), `DELETE FROM driver WHERE rut = $1`, rut); err != nil {
			t.Errorf("clean up test driver: %v", err)
		}
	})

	driver, err := repo.CreateDriver(ctx, domain.Driver{
		Nombre:   "Test Driver",
		Telefono: "555",
		RUT:      rut,
	})
	if err != nil {
		t.Fatal(err)
	}
	if driver.ID == "" || !driver.Activo {
		t.Fatalf("unexpected created driver: %+v", driver)
	}

	if _, err := repo.CreateDriver(ctx, domain.Driver{
		Nombre:   "Duplicate",
		Telefono: "555",
		RUT:      rut,
	}); !errors.Is(err, domain.ErrRUTAlreadyExists) {
		t.Fatalf("expected duplicate RUT error, got %v", err)
	}
	if exists, err := repo.DriverRUTExists(ctx, rut, ""); err != nil || !exists {
		t.Fatalf("expected RUT to exist, exists=%t err=%v", exists, err)
	}
	if exists, err := repo.DriverRUTExists(ctx, rut, driver.ID); err != nil || exists {
		t.Fatalf("expected current driver to be excluded, exists=%t err=%v", exists, err)
	}

	bus, err := repo.CreateBus(ctx, domain.Bus{
		DriverID: driver.ID,
		Patente:  patente,
		Empresa:  "Test Company",
		Estado:   "activo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if bus.ID == "" || bus.DriverID != driver.ID {
		t.Fatalf("unexpected created bus: %+v", bus)
	}
	if _, err := repo.CreateBus(ctx, domain.Bus{
		DriverID: driver.ID,
		Patente:  patente + "-INVALID",
		Empresa:  "Invalid status",
		Estado:   "disponible",
	}); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("expected database status constraint error, got %v", err)
	}
	t.Cleanup(func() {
		if _, err := repo.pool.Exec(context.Background(), `DELETE FROM outbox WHERE payload->'data'->>'busId' = $1`, bus.ID); err != nil {
			t.Errorf("clean up bus outbox events: %v", err)
		}
	})
	var pendingOutboxBefore int
	if err := repo.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE publicado = FALSE`).Scan(&pendingOutboxBefore); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.CreateBus(ctx, domain.Bus{
		DriverID: driver.ID,
		Patente:  patente,
		Empresa:  "Duplicate",
		Estado:   "activo",
	}); !errors.Is(err, domain.ErrPatenteAlreadyExists) {
		t.Fatalf("expected duplicate patente error, got %v", err)
	}
	if exists, err := repo.BusPatenteExists(ctx, patente, ""); err != nil || !exists {
		t.Fatalf("expected patente to exist, exists=%t err=%v", exists, err)
	}
	if exists, err := repo.BusPatenteExists(ctx, patente, bus.ID); err != nil || exists {
		t.Fatalf("expected current bus to be excluded, exists=%t err=%v", exists, err)
	}

	gotBus, err := repo.GetBus(ctx, bus.ID)
	if err != nil || gotBus.ID != bus.ID {
		t.Fatalf("get bus returned %+v, err=%v", gotBus, err)
	}
	buses, err := repo.ListBuses(ctx)
	if err != nil || !containsBus(buses, bus.ID) {
		t.Fatalf("list buses omitted created bus, err=%v", err)
	}

	bus.Empresa = "Updated Company"
	updatedBus, err := repo.UpdateBus(ctx, bus.ID, bus)
	if err != nil || updatedBus.Empresa != bus.Empresa {
		t.Fatalf("update bus returned %+v, err=%v", updatedBus, err)
	}
	var eventCount int
	if err := repo.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE payload->'data'->>'busId' = $1`, bus.ID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("expected no outbox event when status is unchanged, got %d", eventCount)
	}

	bus.Estado = domain.BusStatusMaintenance
	updatedBus, err = repo.UpdateBus(ctx, bus.ID, bus)
	if err != nil || updatedBus.Estado != domain.BusStatusMaintenance {
		t.Fatalf("status update returned %+v, err=%v", updatedBus, err)
	}
	var eventType string
	var eventKey string
	var eventPayload []byte
	err = repo.pool.QueryRow(ctx, `
		SELECT evento, payload->'data'->>'busId', payload
		FROM outbox
		WHERE payload->'data'->>'busId' = $1
		ORDER BY id DESC
		LIMIT 1`, bus.ID).Scan(&eventType, &eventKey, &eventPayload)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		MessageID string `json:"messageId"`
		Type      string `json:"type"`
		Data      struct {
			BusID          string `json:"busId"`
			PreviousEstado string `json:"previousEstado"`
			Estado         string `json:"estado"`
		} `json:"data"`
	}
	if err := json.Unmarshal(eventPayload, &event); err != nil {
		t.Fatal(err)
	}
	if eventType != "bus.status.changed.v1" || event.Type != eventType || event.MessageID == "" {
		t.Fatalf("unexpected outbox event envelope: type=%q event=%+v", eventType, event)
	}
	if eventKey != bus.ID || event.Data.BusID != bus.ID ||
		event.Data.PreviousEstado != domain.BusStatusActive ||
		event.Data.Estado != domain.BusStatusMaintenance {
		t.Fatalf("unexpected bus status event key/data: key=%q data=%+v", eventKey, event.Data)
	}

	bus.Empresa = "Updated without status change"
	if _, err := repo.UpdateBus(ctx, bus.ID, bus); err != nil {
		t.Fatal(err)
	}
	if err := repo.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE payload->'data'->>'busId' = $1`, bus.ID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("expected exactly one status event, got %d", eventCount)
	}
	if pendingOutboxBefore == 0 {
		deliveryFailure := errors.New("Kafka unavailable")
		delivered, err := repo.DeliverNextOutbox(ctx, func(context.Context, []byte, []byte) error {
			return deliveryFailure
		})
		if !delivered || !errors.Is(err, deliveryFailure) {
			t.Fatalf("expected failed publication to retain the outbox event, delivered=%t err=%v", delivered, err)
		}
		var published bool
		if err := repo.pool.QueryRow(ctx, `
			SELECT publicado FROM outbox WHERE payload->'data'->>'busId' = $1`, bus.ID).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if published {
			t.Fatal("outbox event was marked published after a failed Kafka delivery")
		}

		var deliveredKey, deliveredPayload []byte
		delivered, err = repo.DeliverNextOutbox(ctx, func(_ context.Context, key, payload []byte) error {
			deliveredKey = append([]byte(nil), key...)
			deliveredPayload = append([]byte(nil), payload...)
			return nil
		})
		if err != nil || !delivered {
			t.Fatalf("expected outbox event delivery, delivered=%t err=%v", delivered, err)
		}
		if string(deliveredKey) != bus.ID || len(deliveredPayload) == 0 {
			t.Fatalf("unexpected delivered outbox event: key=%q payload=%s", deliveredKey, deliveredPayload)
		}
		if err := repo.pool.QueryRow(ctx, `
			SELECT publicado FROM outbox WHERE payload->'data'->>'busId' = $1`, bus.ID).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if !published {
			t.Fatal("outbox event was not marked as published")
		}
	}
	if err := repo.DeleteBus(ctx, bus.ID); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteBus(ctx, bus.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected not found when deleting bus twice, got %v", err)
	}

	gotDriver, err := repo.GetDriver(ctx, driver.ID)
	if err != nil || gotDriver.ID != driver.ID {
		t.Fatalf("get driver returned %+v, err=%v", gotDriver, err)
	}
	drivers, err := repo.ListDrivers(ctx)
	if err != nil || !containsDriver(drivers, driver.ID) {
		t.Fatalf("list drivers omitted created driver, err=%v", err)
	}

	driver.Nombre = "Updated Driver"
	driver.Activo = true
	updatedDriver, err := repo.UpdateDriver(ctx, driver.ID, driver)
	if err != nil || updatedDriver.Nombre != driver.Nombre {
		t.Fatalf("update driver returned %+v, err=%v", updatedDriver, err)
	}
	deactivatedDriver, err := repo.DeactivateDriver(ctx, driver.ID)
	if err != nil || deactivatedDriver.Activo {
		t.Fatalf("expected driver to be deactivated, got %+v err=%v", deactivatedDriver, err)
	}
}

func containsBus(buses []domain.Bus, id string) bool {
	for _, bus := range buses {
		if bus.ID == id {
			return true
		}
	}
	return false
}

func containsDriver(drivers []domain.Driver, id string) bool {
	for _, driver := range drivers {
		if driver.ID == id {
			return true
		}
	}
	return false
}
