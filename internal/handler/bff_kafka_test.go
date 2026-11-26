package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
	"github.com/segmentio/kafka-go"
)

type bffServiceStub struct {
	BFFService
	createdBus   domain.Bus
	createBusErr error
}

func (s *bffServiceStub) CreateBus(_ context.Context, input domain.BusInput) (domain.Bus, error) {
	s.createdBus = domain.Bus{
		ID:       "123e4567-e89b-12d3-a456-426614174002",
		DriverID: input.DriverID,
		Patente:  input.Patente,
		Empresa:  input.Empresa,
		Estado:   input.Estado,
	}
	return s.createdBus, s.createBusErr
}

func (s *bffServiceStub) CreateDriver(context.Context, domain.DriverInput) (domain.Driver, error) {
	return domain.Driver{}, domain.ErrRUTAlreadyExists
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
