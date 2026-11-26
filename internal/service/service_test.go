package service

import (
	"context"
	"errors"
	"testing"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
)

type repositoryStub struct {
	Repository
	busPatenteExists bool
	driverRUTExists  bool
	busPatenteCheck  string
	driverRUTCheck   string
	excludedID       string
	createdBus       domain.Bus
	createdDriver    domain.Driver
}

func (r *repositoryStub) BusPatenteExists(_ context.Context, patente, exceptID string) (bool, error) {
	r.busPatenteCheck = patente
	r.excludedID = exceptID
	return r.busPatenteExists, nil
}

func (r *repositoryStub) DriverRUTExists(_ context.Context, rut, exceptID string) (bool, error) {
	r.driverRUTCheck = rut
	r.excludedID = exceptID
	return r.driverRUTExists, nil
}

func (r *repositoryStub) CreateBus(_ context.Context, bus domain.Bus) (domain.Bus, error) {
	r.createdBus = bus
	return bus, nil
}

func (r *repositoryStub) UpdateBus(_ context.Context, id string, bus domain.Bus) (domain.Bus, error) {
	bus.ID = id
	r.createdBus = bus
	return bus, nil
}

func (r *repositoryStub) CreateDriver(_ context.Context, driver domain.Driver) (domain.Driver, error) {
	r.createdDriver = driver
	return driver, nil
}

func (r *repositoryStub) UpdateDriver(_ context.Context, id string, driver domain.Driver) (domain.Driver, error) {
	driver.ID = id
	r.createdDriver = driver
	return driver, nil
}

func TestCreateBusNormalizesPatente(t *testing.T) {
	repo := &repositoryStub{}
	svc := New(repo, nil)

	bus, err := svc.CreateBus(context.Background(), domain.BusInput{
		DriverID: "123e4567-e89b-12d3-a456-426614174000",
		Patente:  " ab-123 ",
		Empresa:  " empresa ",
		Estado:   " activo ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if bus.Patente != "AB-123" || repo.createdBus.Patente != "AB-123" {
		t.Fatalf("patente was not normalized: returned=%q stored=%q", bus.Patente, repo.createdBus.Patente)
	}
	if repo.busPatenteCheck != "AB-123" {
		t.Fatalf("duplicate check received patente %q", repo.busPatenteCheck)
	}
}

func TestCreateBusRejectsDuplicatePatente(t *testing.T) {
	repo := &repositoryStub{busPatenteExists: true}
	svc := New(repo, nil)

	_, err := svc.CreateBus(context.Background(), domain.BusInput{
		DriverID: "123e4567-e89b-12d3-a456-426614174000",
		Patente:  "AB-123",
		Empresa:  "Empresa",
		Estado:   "activo",
	})
	if !errors.Is(err, domain.ErrPatenteAlreadyExists) {
		t.Fatalf("expected duplicate patente error, got %v", err)
	}
	if repo.createdBus.ID != "" {
		t.Fatal("repository created a bus with a duplicate patente")
	}
}

func TestCreateBusRejectsUnknownEstado(t *testing.T) {
	repo := &repositoryStub{}
	svc := New(repo, nil)

	_, err := svc.CreateBus(context.Background(), domain.BusInput{
		DriverID: "123e4567-e89b-12d3-a456-426614174000",
		Patente:  "AB-123",
		Empresa:  "Empresa",
		Estado:   "disponible",
	})
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("expected invalid input error, got %v", err)
	}
	if repo.busPatenteCheck != "" {
		t.Fatal("repository should not be called for an invalid bus state")
	}
}

func TestCreateDriverNormalizesAndRejectsDuplicateRUT(t *testing.T) {
	repo := &repositoryStub{driverRUTExists: true}
	svc := New(repo, nil)

	_, err := svc.CreateDriver(context.Background(), domain.DriverInput{
		Nombre:   " Ana ",
		Telefono: " 555 ",
		RUT:      "12.345.678-k",
	})
	if !errors.Is(err, domain.ErrRUTAlreadyExists) {
		t.Fatalf("expected duplicate RUT error, got %v", err)
	}
	if repo.driverRUTCheck != "12345678-K" {
		t.Fatalf("duplicate check received RUT %q", repo.driverRUTCheck)
	}
	if repo.createdDriver.ID != "" {
		t.Fatal("repository created a driver with a duplicate RUT")
	}
}

func TestUpdateBusExcludesCurrentIDFromPatenteCheck(t *testing.T) {
	const id = "123e4567-e89b-12d3-a456-426614174000"
	repo := &repositoryStub{}
	svc := New(repo, nil)

	_, err := svc.UpdateBus(context.Background(), id, domain.BusInput{
		DriverID: "123e4567-e89b-12d3-a456-426614174001",
		Patente:  "AB-123",
		Empresa:  "Empresa",
		Estado:   "activo",
	})
	if err != nil {
		t.Fatal(err)
	}
	if repo.excludedID != id {
		t.Fatalf("expected duplicate check to exclude %q, got %q", id, repo.excludedID)
	}
}
