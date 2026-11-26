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
	busToGet         domain.Bus
	getBusErr        error
	getBusCalls      int
}

func (r *repositoryStub) GetBus(_ context.Context, id string) (domain.Bus, error) {
	r.getBusCalls++
	if r.busToGet.ID == "" && r.getBusErr == nil {
		r.busToGet.ID = id
	}
	return r.busToGet, r.getBusErr
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

func TestValidateBusRequiresExistingActiveBus(t *testing.T) {
	const id = "123e4567-e89b-12d3-a456-426614174000"
	tests := []struct {
		name       string
		bus        domain.Bus
		repoErr    error
		wantErr    bool
		wantValid  bool
		wantEstado string
		wantReason string
	}{
		{
			name:       "active bus",
			bus:        domain.Bus{ID: id, Estado: domain.BusStatusActive},
			wantValid:  true,
			wantEstado: domain.BusStatusActive,
		},
		{
			name:       "inactive bus",
			bus:        domain.Bus{ID: id, Estado: domain.BusStatusMaintenance},
			wantEstado: domain.BusStatusMaintenance,
			wantReason: domain.BusValidationReasonInactive,
		},
		{
			name:       "bus not found",
			repoErr:    domain.ErrNotFound,
			wantReason: domain.BusValidationReasonNotFound,
		},
		{
			name:    "repository error",
			repoErr: errors.New("database unavailable"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &repositoryStub{busToGet: tt.bus, getBusErr: tt.repoErr}
			svc := New(repo, nil)

			result, err := svc.ValidateBus(context.Background(), id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("expected error=%t, got %v", tt.wantErr, err)
			}
			if tt.wantErr {
				return
			}
			if result.Valid != tt.wantValid {
				t.Fatalf("expected valid=%t, got %t", tt.wantValid, result.Valid)
			}
			if tt.wantEstado == "" {
				if result.Estado != nil {
					t.Fatalf("expected no bus status, got %q", *result.Estado)
				}
			} else if result.Estado == nil || *result.Estado != tt.wantEstado {
				t.Fatalf("expected estado %q, got %v", tt.wantEstado, result.Estado)
			}
			if tt.wantReason == "" {
				if result.Reason != nil {
					t.Fatalf("expected no validation reason, got %q", *result.Reason)
				}
			} else if result.Reason == nil || *result.Reason != tt.wantReason {
				t.Fatalf("expected reason %q, got %v", tt.wantReason, result.Reason)
			}
		})
	}
}

func TestValidateBusRejectsInvalidUUIDBeforeRepositoryCall(t *testing.T) {
	repo := &repositoryStub{}
	svc := New(repo, nil)

	_, err := svc.ValidateBus(context.Background(), "not-a-uuid")
	if !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("expected invalid ID error, got %v", err)
	}
	if repo.getBusCalls != 0 {
		t.Fatalf("repository should not be called for invalid UUID, got %d calls", repo.getBusCalls)
	}
}
