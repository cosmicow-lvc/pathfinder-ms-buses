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
	busPatenteErr    error
	driverRUTExists  bool
	driverRUTErr     error
	busPatenteCheck  string
	driverRUTCheck   string
	excludedID       string
	createdBus       domain.Bus
	createBusErr     error
	updateBusErr     error
	deleteBusErr     error
	listBuses        []domain.Bus
	listBusesErr     error
	createdDriver    domain.Driver
	createDriverErr  error
	driverToGet      domain.Driver
	getDriverErr     error
	listDrivers      []domain.Driver
	listDriversErr   error
	updateDriverErr  error
	deactivated      domain.Driver
	deactivateErr    error
	busToGet         domain.Bus
	getBusErr        error
	getBusCalls      int
	pingErr          error
}

func (r *repositoryStub) Ping(context.Context) error {
	return r.pingErr
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
	return r.busPatenteExists, r.busPatenteErr
}

func (r *repositoryStub) DriverRUTExists(_ context.Context, rut, exceptID string) (bool, error) {
	r.driverRUTCheck = rut
	r.excludedID = exceptID
	return r.driverRUTExists, r.driverRUTErr
}

func (r *repositoryStub) CreateBus(_ context.Context, bus domain.Bus) (domain.Bus, error) {
	r.createdBus = bus
	return bus, r.createBusErr
}

func (r *repositoryStub) ListBuses(context.Context) ([]domain.Bus, error) {
	return r.listBuses, r.listBusesErr
}

func (r *repositoryStub) UpdateBus(_ context.Context, id string, bus domain.Bus) (domain.Bus, error) {
	bus.ID = id
	r.createdBus = bus
	return bus, r.updateBusErr
}

func (r *repositoryStub) DeleteBus(context.Context, string) error {
	return r.deleteBusErr
}

func (r *repositoryStub) CreateDriver(_ context.Context, driver domain.Driver) (domain.Driver, error) {
	r.createdDriver = driver
	return driver, r.createDriverErr
}

func (r *repositoryStub) GetDriver(context.Context, string) (domain.Driver, error) {
	return r.driverToGet, r.getDriverErr
}

func (r *repositoryStub) ListDrivers(context.Context) ([]domain.Driver, error) {
	return r.listDrivers, r.listDriversErr
}

func (r *repositoryStub) UpdateDriver(_ context.Context, id string, driver domain.Driver) (domain.Driver, error) {
	driver.ID = id
	r.createdDriver = driver
	return driver, r.updateDriverErr
}

func (r *repositoryStub) DeactivateDriver(context.Context, string) (domain.Driver, error) {
	return r.deactivated, r.deactivateErr
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

type eventPublisherStub struct {
	err   error
	key   []byte
	value []byte
}

func (p *eventPublisherStub) Publish(_ context.Context, key, value []byte) error {
	p.key = append([]byte(nil), key...)
	p.value = append([]byte(nil), value...)
	return p.err
}

func TestServiceReadinessAndEventPublishing(t *testing.T) {
	repo := &repositoryStub{}
	svc := New(repo, nil)
	if err := svc.CheckReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	repo.pingErr = errors.New("database unavailable")
	if err := svc.CheckReady(context.Background()); !errors.Is(err, repo.pingErr) {
		t.Fatalf("expected readiness error, got %v", err)
	}
	if err := svc.Publish(context.Background(), nil, nil); err == nil {
		t.Fatal("expected an error when the publisher is missing")
	}

	publisher := &eventPublisherStub{}
	svc = New(repo, publisher)
	if err := svc.Publish(context.Background(), []byte("bus-id"), []byte("event")); err != nil {
		t.Fatal(err)
	}
	if string(publisher.key) != "bus-id" || string(publisher.value) != "event" {
		t.Fatalf("unexpected published event: key=%q value=%q", publisher.key, publisher.value)
	}

	publisher.err = errors.New("broker unavailable")
	if err := svc.Publish(context.Background(), nil, nil); !errors.Is(err, publisher.err) {
		t.Fatalf("expected publisher error, got %v", err)
	}
}

func TestBusReadAndDeleteOperations(t *testing.T) {
	const id = "123e4567-e89b-12d3-a456-426614174000"
	bus := domain.Bus{ID: id, Estado: domain.BusStatusActive}
	repo := &repositoryStub{busToGet: bus, listBuses: []domain.Bus{bus}}
	svc := New(repo, nil)

	got, err := svc.GetBus(context.Background(), id)
	if err != nil || got.ID != id {
		t.Fatalf("get bus returned %+v, err=%v", got, err)
	}
	if _, err := svc.GetBus(context.Background(), "invalid"); !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("expected invalid ID error, got %v", err)
	}
	repo.getBusErr = domain.ErrNotFound
	if _, err := svc.GetBus(context.Background(), id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected repository not-found error, got %v", err)
	}

	buses, err := svc.ListBuses(context.Background())
	if err != nil || len(buses) != 1 || buses[0].ID != id {
		t.Fatalf("list buses returned %+v, err=%v", buses, err)
	}
	repo.listBusesErr = errors.New("database unavailable")
	if _, err := svc.ListBuses(context.Background()); !errors.Is(err, repo.listBusesErr) {
		t.Fatalf("expected list error, got %v", err)
	}

	if err := svc.DeleteBus(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeleteBus(context.Background(), "invalid"); !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("expected invalid ID on delete, got %v", err)
	}
	repo.deleteBusErr = domain.ErrNotFound
	if err := svc.DeleteBus(context.Background(), id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected delete not-found error, got %v", err)
	}
}

func TestBusWritesPropagateRepositoryErrors(t *testing.T) {
	const (
		id       = "123e4567-e89b-12d3-a456-426614174000"
		driverID = "123e4567-e89b-12d3-a456-426614174001"
	)
	input := domain.BusInput{DriverID: driverID, Patente: "AB-123", Empresa: "Empresa", Estado: domain.BusStatusActive}

	t.Run("create duplicate check error", func(t *testing.T) {
		repo := &repositoryStub{busPatenteErr: errors.New("database unavailable")}
		_, err := New(repo, nil).CreateBus(context.Background(), input)
		if !errors.Is(err, repo.busPatenteErr) {
			t.Fatalf("expected duplicate check error, got %v", err)
		}
	})
	t.Run("create insert error", func(t *testing.T) {
		repo := &repositoryStub{createBusErr: errors.New("database unavailable")}
		_, err := New(repo, nil).CreateBus(context.Background(), input)
		if !errors.Is(err, repo.createBusErr) {
			t.Fatalf("expected insert error, got %v", err)
		}
	})
	t.Run("update duplicate patente", func(t *testing.T) {
		repo := &repositoryStub{busPatenteExists: true}
		_, err := New(repo, nil).UpdateBus(context.Background(), id, input)
		if !errors.Is(err, domain.ErrPatenteAlreadyExists) {
			t.Fatalf("expected duplicate patente error, got %v", err)
		}
	})
	t.Run("update repository error", func(t *testing.T) {
		repo := &repositoryStub{updateBusErr: errors.New("database unavailable")}
		_, err := New(repo, nil).UpdateBus(context.Background(), id, input)
		if !errors.Is(err, repo.updateBusErr) {
			t.Fatalf("expected update error, got %v", err)
		}
	})
}

func TestDriverOperationsNormalizeAndPropagateErrors(t *testing.T) {
	const id = "123e4567-e89b-12d3-a456-426614174000"
	repo := &repositoryStub{
		driverToGet: domain.Driver{ID: id, Nombre: "Ana", Activo: true},
		listDrivers: []domain.Driver{{ID: id}},
		deactivated: domain.Driver{ID: id, Activo: false},
	}
	svc := New(repo, nil)

	created, err := svc.CreateDriver(context.Background(), domain.DriverInput{
		Nombre: " Ana ", Telefono: " 555 ", RUT: "12.345.678-k",
	})
	if err != nil || !created.Activo || repo.createdDriver.RUT != "12345678-K" {
		t.Fatalf("unexpected created driver: %+v err=%v stored=%+v", created, err, repo.createdDriver)
	}
	if _, err := svc.CreateDriver(context.Background(), domain.DriverInput{}); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("expected invalid driver input, got %v", err)
	}

	if _, err := svc.GetDriver(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetDriver(context.Background(), "bad-id"); !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("expected invalid driver ID, got %v", err)
	}
	repo.getDriverErr = domain.ErrNotFound
	if _, err := svc.GetDriver(context.Background(), id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected get driver error, got %v", err)
	}

	drivers, err := svc.ListDrivers(context.Background())
	if err != nil || len(drivers) != 1 {
		t.Fatalf("list drivers returned %+v, err=%v", drivers, err)
	}
	repo.listDriversErr = errors.New("database unavailable")
	if _, err := svc.ListDrivers(context.Background()); !errors.Is(err, repo.listDriversErr) {
		t.Fatalf("expected list drivers error, got %v", err)
	}

	updated, err := svc.UpdateDriver(context.Background(), id, domain.DriverUpdateInput{
		Nombre: " Ana ", Telefono: " 555 ", RUT: "12.345.678-k",
	})
	if err != nil || updated.ID != id || updated.Activo {
		t.Fatalf("unexpected updated driver: %+v err=%v", updated, err)
	}
	if _, err := svc.UpdateDriver(context.Background(), "bad-id", domain.DriverUpdateInput{}); !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("expected invalid driver ID on update, got %v", err)
	}
	repo.driverRUTErr = errors.New("database unavailable")
	if _, err := svc.UpdateDriver(context.Background(), id, domain.DriverUpdateInput{
		Nombre: "Ana", Telefono: "555", RUT: "12345678-K",
	}); !errors.Is(err, repo.driverRUTErr) {
		t.Fatalf("expected RUT check error, got %v", err)
	}
	repo.driverRUTErr = nil
	repo.driverRUTExists = true
	if _, err := svc.UpdateDriver(context.Background(), id, domain.DriverUpdateInput{
		Nombre: "Ana", Telefono: "555", RUT: "12345678-K",
	}); !errors.Is(err, domain.ErrRUTAlreadyExists) {
		t.Fatalf("expected duplicate RUT error, got %v", err)
	}
	repo.driverRUTExists = false
	repo.updateDriverErr = errors.New("database unavailable")
	if _, err := svc.UpdateDriver(context.Background(), id, domain.DriverUpdateInput{
		Nombre: "Ana", Telefono: "555", RUT: "12345678-K",
	}); !errors.Is(err, repo.updateDriverErr) {
		t.Fatalf("expected update driver error, got %v", err)
	}

	deactivated, err := svc.DeactivateDriver(context.Background(), id)
	if err != nil || deactivated.Activo {
		t.Fatalf("expected inactive driver, got %+v err=%v", deactivated, err)
	}
	if _, err := svc.DeactivateDriver(context.Background(), "bad-id"); !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("expected invalid driver ID on deactivation, got %v", err)
	}
	repo.deactivateErr = domain.ErrNotFound
	if _, err := svc.DeactivateDriver(context.Background(), id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected deactivation error, got %v", err)
	}
}
