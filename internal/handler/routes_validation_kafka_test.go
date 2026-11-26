package handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
	"github.com/segmentio/kafka-go"
)

type routeValidationServiceStub struct {
	result domain.BusValidation
	err    error
	busID  string
}

func (s *routeValidationServiceStub) ValidateBus(_ context.Context, busID string) (domain.BusValidation, error) {
	s.busID = busID
	return s.result, s.err
}

func TestRoutesValidationKafkaHandlerRepliesWithCorrelatedValidation(t *testing.T) {
	const (
		correlationID = "f289fe5e-56c4-4a56-a439-23a03d4e4cc0"
		busID         = "123e4567-e89b-12d3-a456-426614174000"
	)
	status := domain.BusStatusActive
	svc := &routeValidationServiceStub{
		result: domain.BusValidation{BusID: busID, Valid: true, Estado: &status},
	}
	publisher := &replyPublisherStub{}
	h, err := NewRoutesValidationKafkaHandler(svc, publisher, []string{"routes.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"messageId":"3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
		"correlationId":"` + correlationID + `",
		"type":"bus.validate.v1",
		"replyTo":"routes.replies.v1",
		"data":{"busId":"` + busID + `"}
	}`
	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}

	if svc.busID != busID {
		t.Fatalf("expected validation for bus %q, got %q", busID, svc.busID)
	}
	if publisher.topic != "routes.replies.v1" || string(publisher.key) != correlationID {
		t.Fatalf("unexpected reply destination/key: topic=%q key=%q", publisher.topic, publisher.key)
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.CorrelationID != correlationID || response.Type != "bus.validate.result.v1" {
		t.Fatalf("unexpected response envelope: %+v", response)
	}
	if response.Error != nil {
		t.Fatalf("expected successful validation response, got %+v", response.Error)
	}
	data, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var validation domain.BusValidation
	if err := json.Unmarshal(data, &validation); err != nil {
		t.Fatal(err)
	}
	if !validation.Valid || validation.BusID != busID || validation.Estado == nil || *validation.Estado != status {
		t.Fatalf("unexpected validation response: %+v", validation)
	}
}

func TestRoutesValidationKafkaHandlerSupportsModernBusValidateType(t *testing.T) {
	status := domain.BusStatusActive
	svc := &routeValidationServiceStub{
		result: domain.BusValidation{
			BusID:     "123e4567-e89b-12d3-a456-426614174000",
			Valid:     true,
			Exists:    true,
			Available: true,
			Estado:    &status,
		},
	}
	publisher := &replyPublisherStub{}
	h, err := NewRoutesValidationKafkaHandler(svc, publisher, []string{"bus.validate.reply"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"messageId":"modern-request",
		"correlationId":"modern-correlation",
		"type":"bus.validate",
		"replyTo":"bus.validate.reply",
		"data":{"busId":"123e4567-e89b-12d3-a456-426614174000"}
	}`
	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || response.CorrelationID != "modern-correlation" {
		t.Fatalf("unexpected modern validation response: %+v", response)
	}
}

func TestRoutesValidationKafkaHandlerReturnsInvalidRequestForMissingBusID(t *testing.T) {
	svc := &routeValidationServiceStub{}
	publisher := &replyPublisherStub{}
	h, err := NewRoutesValidationKafkaHandler(svc, publisher, []string{"routes.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"messageId":"3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
		"correlationId":"f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
		"type":"bus.validate.v1",
		"replyTo":"routes.replies.v1",
		"data":{}
	}`
	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}
	if svc.busID != "" {
		t.Fatalf("service should not be called without busId, got %q", svc.busID)
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected INVALID_REQUEST response, got %+v", response.Error)
	}
}

func TestRoutesValidationKafkaHandlerReturnsInternalError(t *testing.T) {
	svc := &routeValidationServiceStub{err: errors.New("database unavailable")}
	publisher := &replyPublisherStub{}
	h, err := NewRoutesValidationKafkaHandler(svc, publisher, []string{"routes.replies.v1"})
	if err != nil {
		t.Fatal(err)
	}

	request := `{
		"messageId":"3e46c388-8fce-4bfc-a86b-1c35b3e1ffed",
		"correlationId":"f289fe5e-56c4-4a56-a439-23a03d4e4cc0",
		"type":"bus.validate.v1",
		"replyTo":"routes.replies.v1",
		"data":{"busId":"123e4567-e89b-12d3-a456-426614174000"}
	}`
	if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
		t.Fatal(err)
	}

	var response responseEnvelope
	if err := json.Unmarshal(publisher.payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "INTERNAL_ERROR" {
		t.Fatalf("expected INTERNAL_ERROR response, got %+v", response.Error)
	}
}
func TestRoutesValidationKafkaHandlerReportsUnsupportedAndPublishErrors(t *testing.T) {
	t.Run("unsupported operation", func(t *testing.T) {
		publisher := &replyPublisherStub{}
		h, err := NewRoutesValidationKafkaHandler(&routeValidationServiceStub{}, publisher, []string{"routes.replies.v1"})
		if err != nil {
			t.Fatal(err)
		}
		request := `{
			"messageId":"m",
			"correlationId":"c",
			"type":"bus.other.v1",
			"replyTo":"routes.replies.v1",
			"data":{}
		}`
		if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); err != nil {
			t.Fatal(err)
		}
		var response responseEnvelope
		if err := json.Unmarshal(publisher.payload, &response); err != nil {
			t.Fatal(err)
		}
		if response.Error == nil || response.Error.Code != "UNSUPPORTED_OPERATION" {
			t.Fatalf("expected unsupported operation response, got %+v", response.Error)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		publisher := &replyPublisherStub{err: errors.New("broker unavailable")}
		h, err := NewRoutesValidationKafkaHandler(&routeValidationServiceStub{}, publisher, []string{"routes.replies.v1"})
		if err != nil {
			t.Fatal(err)
		}
		request := `{
			"messageId":"m",
			"correlationId":"c",
			"type":"bus.validate.v1",
			"replyTo":"routes.replies.v1",
			"data":{"busId":"123e4567-e89b-12d3-a456-426614174000"}
		}`
		if err := h.Handle(context.Background(), kafka.Message{Value: []byte(request)}); !errors.Is(err, publisher.err) {
			t.Fatalf("expected publish error, got %v", err)
		}
	})
}
