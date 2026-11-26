package handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
	"github.com/segmentio/kafka-go"
)

type bffServiceStub struct {
	BFFService
	createdBus   domain.Bus
	createBusErr error
	driverErr    error
	called       string
}

func (s *bffServiceStub) CreateBus(_ context.Context, input domain.BusInput) (domain.Bus, error) {
	s.called = "bus.create.v1"
	s.createdBus = domain.Bus{
		ID:       "123e4567-e89b-12d3-a456-426614174002",
		DriverID: input.DriverID,
		Patente:  input.Patente,
		Empresa:  input.Empresa,
		Estado:   input.Estado,
	}
	return s.createdBus, s.createBusErr
}

func (s *bffServiceStub) GetBus(_ context.Context, id string) (domain.Bus, error) {
	s.called = "bus.get.v1"
	return domain.Bus{ID: id}, nil
}

func (s *bffServiceStub) ListBuses(context.Context) ([]domain.Bus, error) {
	s.called = "bus.list.v1"
	return []domain.Bus{}, nil
}

func (s *bffServiceStub) UpdateBus(_ context.Context, id string, input domain.BusInput) (domain.Bus, error) {
	s.called = "bus.update.v1"
	return domain.Bus{
		ID:       id,
		DriverID: input.DriverID,
		Patente:  input.Patente,
		Empresa:  input.Empresa,
		Estado:   input.Estado,
	}, nil
}

func (s *bffServiceStub) DeleteBus(context.Context, string) error {
	s.called = "bus.delete.v1"
	return nil
}

func (s *bffServiceStub) CreateDriver(_ context.Context, input domain.DriverInput) (domain.Driver, error) {
	s.called = "driver.create.v1"
	return domain.Driver{Nombre: input.Nombre}, s.driverErr
}

func (s *bffServiceStub) GetDriver(_ context.Context, id string) (domain.Driver, error) {
	s.called = "driver.get.v1"
	return domain.Driver{ID: id}, nil
}

func (s *bffServiceStub) ListDrivers(context.Context) ([]domain.Driver, error) {
	s.called = "driver.list.v1"
	return []domain.Driver{}, nil
}

func (s *bffServiceStub) UpdateDriver(_ context.Context, id string, input domain.DriverUpdateInput) (domain.Driver, error) {
	s.called = "driver.update.v1"
	return domain.Driver{ID: id, Nombre: input.Nombre}, nil
}

func (s *bffServiceStub) DeactivateDriver(_ context.Context, id string) (domain.Driver, error) {
	s.called = "driver.deactivate.v1"
	return domain.Driver{ID: id}, nil
}

type replyPublisherStub struct {
	topic   string
	key     []byte
	payload []byte
	err     error
}

func (p *replyPublisherStub) PublishTo(_ context.Context, topic string, key, payload []byte) error {
	p.topic = topic
	p.key = append([]byte(nil), key...)
	p.payload = append([]byte(nil), payload...)
	return p.err
}

func TestBFFKafkaHandlerRepliesWithCorrelationID(t *testing.T) {
	svc := &bffServiceStub{}
	publisher := &replyPublisherStub{}
	h, err := NewBFFKafkaHandler(svc, publisher, []string{"bff.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	const correlationID = "f289fe5e-56c4-4a56-a439-23a03d4e4cc0"
	request := `{
		"messageId":"3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
		"correlationId":"` + correlationID + `",
		"type":"bus.create.v1",
		"replyTo":"bff.replies.v1",
		"sentAt":"2026-11-25T18:00:00Z",
		"data":{
			"driverId":"123e4567-e89b-12d3-a456-426614174000",
			"patente":"AB-123",
			"empresa":"Empresa",
			"estado":"activo"
		}
	}`

	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}
	if publisher.topic != "bff.replies.v1" {
		t.Fatalf("expected reply topic bff.replies.v1, got %q", publisher.topic)
	}
	if string(publisher.key) != correlationID {
		t.Fatalf("expected reply key %q, got %q", correlationID, publisher.key)
	}
	if svc.createdBus.Patente != "AB-123" {
		t.Fatalf("unexpected bus command input: %+v", svc.createdBus)
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.CorrelationID != correlationID || response.Type != "bus.create.result.v1" {
		t.Fatalf("unexpected response envelope: %+v", response)
	}
	if response.Error != nil {
		t.Fatalf("expected successful response, got %+v", response.Error)
	}
}

func TestBFFKafkaHandlerRepliesWithDomainError(t *testing.T) {
	svc := &bffServiceStub{createBusErr: domain.ErrPatenteAlreadyExists}
	publisher := &replyPublisherStub{}
	h, err := NewBFFKafkaHandler(svc, publisher, []string{"bff.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"messageId":"3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
		"correlationId":"f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
		"type":"bus.create.v1",
		"replyTo":"bff.replies.v1",
		"data":{
			"driverId":"123e4567-e89b-12d3-a456-426614174000",
			"patente":"AB-123",
			"empresa":"Empresa",
			"estado":"activo"
		}
	}`
	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "PATENTE_ALREADY_EXISTS" {
		t.Fatalf("expected duplicate patente response, got %+v", response.Error)
	}
	if response.Data != nil {
		t.Fatalf("expected data to be null for an error, got %#v", response.Data)
	}
}

func TestBFFKafkaHandlerRejectsUnapprovedReplyTopic(t *testing.T) {
	h, err := NewBFFKafkaHandler(&bffServiceStub{}, &replyPublisherStub{}, []string{"bff.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"messageId":"3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
		"correlationId":"f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
		"type":"bus.list.v1",
		"replyTo":"unapproved.replies.v1",
		"data":{}
	}`
	err = h.Handle(context.Background(), kafka.Message{Value: []byte(request)})
	if err == nil {
		t.Fatalf("expected error for unapproved reply topic, got %v", err)
	}
}

func TestBFFKafkaHandlerRepliesInvalidRequestWithoutDispatch(t *testing.T) {
	svc := &bffServiceStub{}
	publisher := &replyPublisherStub{}
	h, err := NewBFFKafkaHandler(svc, publisher, []string{"bff.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"correlationId":"f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
		"type":"bus.create.v1",
		"replyTo":"bff.replies.v1",
		"data":{
			"driverId":"123e4567-e89b-12d3-a456-426614174000",
			"patente":"AB-123",
			"empresa":"Empresa",
			"estado":"activo"
		}
	}`
	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}
	if svc.createdBus.ID != "" {
		t.Fatal("invalid request must not execute a command")
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected INVALID_REQUEST response, got %+v", response.Error)
	}
}

func TestBFFKafkaHandlerDispatchesAllSupportedOperations(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantCalled string
	}{
		{name: "buses.bus.create", data: `{"patente":"AB-123","empresa":"Empresa","driverId":"123e4567-e89b-12d3-a456-426614174000"}`, wantCalled: "bus.create.v1"},
		{name: "buses.bus.change-status", data: `{"id":"123e4567-e89b-12d3-a456-426614174000","estado":"EN_MANTENCION"}`, wantCalled: "bus.update.v1"},
		{name: "bus.create.v1", data: `{"driverId":"123e4567-e89b-12d3-a456-426614174000","patente":"AB-123","empresa":"Empresa","estado":"activo"}`, wantCalled: "bus.create.v1"},
		{name: "bus.get.v1", data: `{"busId":"123e4567-e89b-12d3-a456-426614174000"}`},
		{name: "bus.list.v1", data: `{}`},
		{name: "bus.update.v1", data: `{"busId":"123e4567-e89b-12d3-a456-426614174000","driverId":"123e4567-e89b-12d3-a456-426614174001","patente":"AB-123","empresa":"Empresa","estado":"activo"}`},
		{name: "bus.delete.v1", data: `{"busId":"123e4567-e89b-12d3-a456-426614174000"}`},
		{name: "driver.create.v1", data: `{"nombre":"Ana","telefono":"555","rut":"12345678-K"}`},
		{name: "driver.get.v1", data: `{"driverId":"123e4567-e89b-12d3-a456-426614174000"}`},
		{name: "driver.list.v1", data: `{}`},
		{name: "driver.update.v1", data: `{"driverId":"123e4567-e89b-12d3-a456-426614174000","nombre":"Ana","telefono":"555","rut":"12345678-K","activo":true}`},
		{name: "driver.deactivate.v1", data: `{"driverId":"123e4567-e89b-12d3-a456-426614174000"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &bffServiceStub{}
			publisher := &replyPublisherStub{}
			h, err := NewBFFKafkaHandler(svc, publisher, []string{"bff.replies.v1"})
			if err != nil {
				t.Fatal(err)
			}
			request, err := json.Marshal(requestEnvelope{
				MessageID:     "message-1",
				CorrelationID: "correlation-1",
				Type:          tt.name,
				ReplyTo:       "bff.replies.v1",
				Data:          json.RawMessage(tt.data),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Handle(context.Background(), kafka.Message{Value: request}); err != nil {
				t.Fatal(err)
			}
			wantCalled := tt.wantCalled
			if wantCalled == "" {
				wantCalled = tt.name
			}
			if svc.called != wantCalled {
				t.Fatalf("expected operation %q to be dispatched, got %q", wantCalled, svc.called)
			}
			var response responseEnvelope
			if err := json.Unmarshal(publisher.payload, &response); err != nil {
				t.Fatal(err)
			}
			if response.Error != nil {
				t.Fatalf("expected successful operation response, got %+v", response.Error)
			}
			if response.Type != resultType(tt.name) {
				t.Fatalf("expected response type %q, got %q", resultType(tt.name), response.Type)
			}
		})
	}
}

func TestBFFKafkaHandlerReportsUnsupportedAndMalformedRequests(t *testing.T) {
	for _, tt := range []struct {
		name string
		body string
		code string
	}{
		{
			name: "unsupported operation",
			body: `{"messageId":"m","correlationId":"c","type":"unknown.v1","replyTo":"bff.replies.v1","data":{}}`,
			code: "UNSUPPORTED_OPERATION",
		},
		{
			name: "invalid data",
			body: `{"messageId":"m","correlationId":"c","type":"bus.get.v1","replyTo":"bff.replies.v1","data":"invalid"}`,
			code: "INVALID_REQUEST",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &replyPublisherStub{}
			h, err := NewBFFKafkaHandler(&bffServiceStub{}, publisher, []string{"bff.replies.v1"})
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Handle(context.Background(), kafka.Message{Value: []byte(tt.body)}); err != nil {
				t.Fatal(err)
			}
			var response responseEnvelope
			if err := json.Unmarshal(publisher.payload, &response); err != nil {
				t.Fatal(err)
			}
			if response.Error == nil || response.Error.Code != tt.code {
				t.Fatalf("expected error %q, got %+v", tt.code, response.Error)
			}
		})
	}
}

func TestCommandErrorMapsDomainErrors(t *testing.T) {
	tests := []struct {
		err  error
		code string
	}{
		{err: domain.ErrInvalidID, code: "INVALID_REQUEST"},
		{err: domain.ErrInvalidInput, code: "INVALID_REQUEST"},
		{err: domain.ErrNotFound, code: "NOT_FOUND"},
		{err: domain.ErrDriverNotFound, code: "DRIVER_NOT_FOUND"},
		{err: domain.ErrPatenteAlreadyExists, code: "PATENTE_ALREADY_EXISTS"},
		{err: domain.ErrRUTAlreadyExists, code: "RUT_ALREADY_EXISTS"},
		{err: errUnsupportedOperation, code: "UNSUPPORTED_OPERATION"},
		{err: errors.New("database unavailable"), code: "INTERNAL_ERROR"},
	}

	for _, tt := range tests {
		got := commandError(tt.err, "test.operation.v1")
		if got.Code != tt.code {
			t.Errorf("commandError(%v) = %q, want %q", tt.err, got.Code, tt.code)
		}
	}
}
