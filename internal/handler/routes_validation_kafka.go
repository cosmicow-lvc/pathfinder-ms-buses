package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
	"github.com/segmentio/kafka-go"
)

type RouteValidationService interface {
	ValidateBus(context.Context, string) (domain.BusValidation, error)
}

type RoutesValidationKafkaHandler struct {
	service        RouteValidationService
	publisher      KafkaReplyPublisher
	allowedReplies map[string]struct{}
}

func NewRoutesValidationKafkaHandler(
	svc RouteValidationService,
	publisher KafkaReplyPublisher,
	replyTopics []string,
) (*RoutesValidationKafkaHandler, error) {
	if svc == nil {
		return nil, errors.New("bus validation service is required")
	}
	if publisher == nil {
		return nil, errors.New("Kafka reply publisher is required")
	}

	allowedReplies := make(map[string]struct{}, len(replyTopics))
	for _, topic := range replyTopics {
		if topic = strings.TrimSpace(topic); topic != "" {
			allowedReplies[topic] = struct{}{}
		}
	}
	if len(allowedReplies) == 0 {
		return nil, errors.New("at least one Kafka reply topic is required")
	}
	return &RoutesValidationKafkaHandler{
		service:        svc,
		publisher:      publisher,
		allowedReplies: allowedReplies,
	}, nil
}

func (h *RoutesValidationKafkaHandler) Handle(ctx context.Context, message kafka.Message) error {
	var request requestEnvelope
	if err := json.Unmarshal(message.Value, &request); err != nil {
		return nil
	}
	if request.Type == "" && (message.Topic == "bus.validate" || message.Topic == "buses.validation.requests.v1") {
		request.Type = "bus.validate"
		if len(request.Data) == 0 {
			request.Data = message.Value
		}
	}
	if request.CorrelationID == "" {
		request.CorrelationID = headerValue(message.Headers, "x-correlation-id")
		if request.CorrelationID == "" {
			request.CorrelationID = headerValue(message.Headers, "kafka_correlationId")
		}
	}
	if request.ReplyTo == "" {
		request.ReplyTo = headerValue(message.Headers, "kafka_replyTopic")
	}
	if request.CorrelationID == "" {
		if generated, err := newMessageID(); err == nil {
			request.CorrelationID = generated
		}
	}
	if request.CorrelationID == "" || request.ReplyTo == "" {
		return errors.New("Routes validation request is missing correlationId or replyTo")
	}
	if _, ok := h.allowedReplies[request.ReplyTo]; !ok {
		return fmt.Errorf("reply topic %q is not allowed", request.ReplyTo)
	}

	result := commandResult{err: domain.ErrInvalidInput}
	if request.MessageID != "" && (request.Type == "bus.validate.v1" || request.Type == "bus.validate") {
		var input struct {
			BusID string `json:"busId"`
		}
		if err := decodeData(request.Data, &input); err != nil {
			result.err = err
		} else if strings.TrimSpace(input.BusID) == "" {
			result.err = domain.ErrInvalidInput
		} else {
			result.data, result.err = h.service.ValidateBus(ctx, input.BusID)
		}
	} else if request.MessageID != "" && request.Type != "" {
		result.err = errUnsupportedOperation
	}

	reply, err := newResponse(request, result)
	if err != nil {
		return fmt.Errorf("build Routes validation response: %w", err)
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("encode Routes validation response: %w", err)
	}
	if err := h.publisher.PublishTo(ctx, request.ReplyTo, []byte(request.CorrelationID), payload); err != nil {
		return fmt.Errorf("publish Routes validation response: %w", err)
	}
	return nil
}
