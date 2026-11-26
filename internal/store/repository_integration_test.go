package store

import (
	"context"
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
		if _, err := repo.pool.Exec(context.Background(), `DELETE FROM buses WHERE patente = $1`, patente); err != nil {
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
