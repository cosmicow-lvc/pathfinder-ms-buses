package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
)

type Repository interface {
	Ping(context.Context) error
	CreateBus(context.Context, domain.Bus) (domain.Bus, error)
	GetBus(context.Context, string) (domain.Bus, error)
	ListBuses(context.Context) ([]domain.Bus, error)
	UpdateBus(context.Context, string, domain.Bus) (domain.Bus, error)
	DeleteBus(context.Context, string) error
	BusPatenteExists(context.Context, string, string) (bool, error)
	CreateDriver(context.Context, domain.Driver) (domain.Driver, error)
	GetDriver(context.Context, string) (domain.Driver, error)
	ListDrivers(context.Context) ([]domain.Driver, error)
	UpdateDriver(context.Context, string, domain.Driver) (domain.Driver, error)
	DeactivateDriver(context.Context, string) (domain.Driver, error)
	DriverRUTExists(context.Context, string, string) (bool, error)
}

type EventPublisher interface {
	Publish(context.Context, []byte, []byte) error
}

type Service struct {
	repo      Repository
	publisher EventPublisher
}

func New(repo Repository, publisher EventPublisher) *Service {
	return &Service{repo: repo, publisher: publisher}
}

func (s *Service) CheckReady(ctx context.Context) error {
	return s.repo.Ping(ctx)
}

func (s *Service) Publish(ctx context.Context, key, value []byte) error {
	if s.publisher == nil {
		return errors.New("event publisher is not configured")
	}
	return s.publisher.Publish(ctx, key, value)
}

func (s *Service) CreateBus(ctx context.Context, input domain.BusInput) (domain.Bus, error) {
	bus, err := normalizeBus(input)
	if err != nil {
		return domain.Bus{}, err
	}
	exists, err := s.repo.BusPatenteExists(ctx, bus.Patente, "")
	if err != nil {
		return domain.Bus{}, fmt.Errorf("check bus patente: %w", err)
	}
	if exists {
		return domain.Bus{}, domain.ErrPatenteAlreadyExists
	}
	return s.repo.CreateBus(ctx, bus)
}

func (s *Service) GetBus(ctx context.Context, id string) (domain.Bus, error) {
	if !validUUID(id) {
		return domain.Bus{}, domain.ErrInvalidID
	}
	return s.repo.GetBus(ctx, id)
}

func (s *Service) ListBuses(ctx context.Context) ([]domain.Bus, error) {
	return s.repo.ListBuses(ctx)
}

func (s *Service) UpdateBus(ctx context.Context, id string, input domain.BusInput) (domain.Bus, error) {
	if !validUUID(id) {
		return domain.Bus{}, domain.ErrInvalidID
	}
	bus, err := normalizeBus(input)
	if err != nil {
		return domain.Bus{}, err
	}
	exists, err := s.repo.BusPatenteExists(ctx, bus.Patente, id)
	if err != nil {
		return domain.Bus{}, fmt.Errorf("check bus patente: %w", err)
	}
	if exists {
		return domain.Bus{}, domain.ErrPatenteAlreadyExists
	}
	return s.repo.UpdateBus(ctx, id, bus)
}

func (s *Service) DeleteBus(ctx context.Context, id string) error {
	if !validUUID(id) {
		return domain.ErrInvalidID
	}
	return s.repo.DeleteBus(ctx, id)
}

func (s *Service) CreateDriver(ctx context.Context, input domain.DriverInput) (domain.Driver, error) {
	driver, err := normalizeDriver(input.Nombre, input.Telefono, input.RUT)
	if err != nil {
		return domain.Driver{}, err
	}
	exists, err := s.repo.DriverRUTExists(ctx, driver.RUT, "")
	if err != nil {
		return domain.Driver{}, fmt.Errorf("check driver RUT: %w", err)
	}
	if exists {
		return domain.Driver{}, domain.ErrRUTAlreadyExists
	}
	driver.Activo = true
	return s.repo.CreateDriver(ctx, driver)
}

func (s *Service) GetDriver(ctx context.Context, id string) (domain.Driver, error) {
	if !validUUID(id) {
		return domain.Driver{}, domain.ErrInvalidID
	}
	return s.repo.GetDriver(ctx, id)
}

func (s *Service) ListDrivers(ctx context.Context) ([]domain.Driver, error) {
	return s.repo.ListDrivers(ctx)
}

func (s *Service) UpdateDriver(ctx context.Context, id string, input domain.DriverUpdateInput) (domain.Driver, error) {
	if !validUUID(id) {
		return domain.Driver{}, domain.ErrInvalidID
	}
	driver, err := normalizeDriver(input.Nombre, input.Telefono, input.RUT)
	if err != nil {
		return domain.Driver{}, err
	}
	exists, err := s.repo.DriverRUTExists(ctx, driver.RUT, id)
	if err != nil {
		return domain.Driver{}, fmt.Errorf("check driver RUT: %w", err)
	}
	if exists {
		return domain.Driver{}, domain.ErrRUTAlreadyExists
	}
	driver.Activo = input.Activo
	return s.repo.UpdateDriver(ctx, id, driver)
}

func (s *Service) DeactivateDriver(ctx context.Context, id string) (domain.Driver, error) {
	if !validUUID(id) {
		return domain.Driver{}, domain.ErrInvalidID
	}
	return s.repo.DeactivateDriver(ctx, id)
}

func normalizeBus(input domain.BusInput) (domain.Bus, error) {
	bus := domain.Bus{
		DriverID: strings.TrimSpace(input.DriverID),
		Patente:  strings.ToUpper(strings.TrimSpace(input.Patente)),
		Empresa:  strings.TrimSpace(input.Empresa),
		Estado:   strings.TrimSpace(input.Estado),
	}
	if !validUUID(bus.DriverID) {
		return domain.Bus{}, fmt.Errorf("%w: driver_id", domain.ErrInvalidInput)
	}
	if bus.Patente == "" || bus.Empresa == "" || bus.Estado == "" {
		return domain.Bus{}, domain.ErrInvalidInput
	}
	switch bus.Estado {
	case domain.BusStatusActive, domain.BusStatusInactive, domain.BusStatusMaintenance:
	default:
		return domain.Bus{}, domain.ErrInvalidInput
	}
	return bus, nil
}

func normalizeDriver(nombre, telefono, rut string) (domain.Driver, error) {
	driver := domain.Driver{
		Nombre:   strings.TrimSpace(nombre),
		Telefono: strings.TrimSpace(telefono),
		RUT:      normalizeRUT(rut),
	}
	if driver.Nombre == "" || driver.Telefono == "" || driver.RUT == "" {
		return domain.Driver{}, domain.ErrInvalidInput
	}
	return driver, nil
}

func normalizeRUT(rut string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(rut), ".", ""))
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i, char := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}
