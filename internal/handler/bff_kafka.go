package handler

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/domain"
	"github.com/segmentio/kafka-go"
)

type BFFService interface {
	CreateBus(context.Context, domain.BusInput) (domain.Bus, error)
	GetBus(context.Context, string) (domain.Bus, error)
	ListBuses(context.Context) ([]domain.Bus, error)
	UpdateBus(context.Context, string, domain.BusInput) (domain.Bus, error)
	DeleteBus(context.Context, string) error
	CreateDriver(context.Context, domain.DriverInput) (domain.Driver, error)
	GetDriver(context.Context, string) (domain.Driver, error)
	ListDrivers(context.Context) ([]domain.Driver, error)
	UpdateDriver(context.Context, string, domain.DriverUpdateInput) (domain.Driver, error)
	DeactivateDriver(context.Context, string) (domain.Driver, error)
}

type KafkaReplyPublisher interface {
	PublishTo(context.Context, string, []byte, []byte) error
}

type BFFKafkaHandler struct {
	service        BFFService
	publisher      KafkaReplyPublisher
	allowedReplies map[string]struct{}
}

type requestEnvelope struct {
	MessageID     string          `json:"messageId"`
	CorrelationID string          `json:"correlationId"`
	Type          string          `json:"type"`
	ReplyTo       string          `json:"replyTo"`
	SentAt        string          `json:"sentAt"`
	Data          json.RawMessage `json:"data"`
}

type responseEnvelope struct {
	MessageID     string         `json:"messageId"`
	CorrelationID string         `json:"correlationId"`
	Type          string         `json:"type"`
	SentAt        string         `json:"sentAt"`
	Ok            *bool          `json:"ok,omitempty"`
	Data          any            `json:"data"`
	Error         *responseError `json:"error"`
}

type responseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type commandResult struct {
	data any
	err  error
}

var errUnsupportedOperation = errors.New("unsupported operation")

func NewBFFKafkaHandler(svc BFFService, publisher KafkaReplyPublisher, replyTopics []string) (*BFFKafkaHandler, error) {
	if svc == nil {
		return nil, errors.New("BFF service is required")
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
	return &BFFKafkaHandler{
		service:        svc,
		publisher:      publisher,
		allowedReplies: allowedReplies,
	}, nil
}

func (h *BFFKafkaHandler) Handle(ctx context.Context, message kafka.Message) error {
	var request requestEnvelope
	if err := json.Unmarshal(message.Value, &request); err != nil {
		slog.Error("invalid BFF Kafka JSON; skipping message", "err", err)
		return nil
	}
	if request.Type == "" {
		request.Type = bffOperationFromTopic(message.Topic)
		if request.Type != "" && len(request.Data) == 0 {
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
		return errors.New("BFF Kafka request is missing correlationId or replyTo")
	}
	if _, ok := h.allowedReplies[request.ReplyTo]; !ok {
		return fmt.Errorf("reply topic %q is not allowed", request.ReplyTo)
	}

	var result commandResult
	if request.MessageID == "" || request.Type == "" {
		result = commandResult{err: domain.ErrInvalidInput}
	} else {
		result = h.dispatch(ctx, request)
	}
	reply, err := newResponse(request, result)
	if err != nil {
		return fmt.Errorf("build BFF Kafka response: %w", err)
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("encode BFF Kafka response: %w", err)
	}
	if err := h.publisher.PublishTo(ctx, request.ReplyTo, []byte(request.CorrelationID), payload); err != nil {
		return fmt.Errorf("publish BFF Kafka response: %w", err)
	}
	return nil
}

func bffOperationFromTopic(topic string) string {
	switch topic {
	case "buses.driver.list":
		return "buses.driver.list"
	case "buses.driver.create":
		return "buses.driver.create"
	case "buses.bus.list":
		return "buses.bus.list"
	case "buses.bus.get":
		return "buses.bus.get"
	case "buses.bus.create":
		return "buses.bus.create"
	case "buses.bus.change-status":
		return "buses.bus.change-status"
	default:
		return ""
	}
}

func (h *BFFKafkaHandler) dispatch(ctx context.Context, request requestEnvelope) commandResult {
	var data any
	var err error

	switch request.Type {
	case "bus.create.v1", "buses.bus.create", "bus.create":
		var input domain.BusInput
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.CreateBus(ctx, input)
		}
	case "bus.get.v1", "buses.bus.get", "bus.get":
		var input struct {
			BusID string `json:"busId"`
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.GetBus(ctx, input.BusID)
		}
	case "bus.list.v1", "buses.bus.list", "bus.list":
		var buses []domain.Bus
		buses, err = h.service.ListBuses(ctx)
		if err == nil {
			var input struct {
				IDs    []string `json:"ids"`
				Estado string   `json:"estado"`
			}
			if len(request.Data) > 0 {
				_ = decodeData(request.Data, &input)
			}
			if len(input.IDs) > 0 {
				allowed := make(map[string]struct{}, len(input.IDs))
				for _, id := range input.IDs {
					allowed[id] = struct{}{}
				}
				filtered := make([]domain.Bus, 0, len(buses))
				for _, bus := range buses {
					if _, ok := allowed[bus.ID]; ok {
						filtered = append(filtered, bus)
					}
				}
				buses = filtered
			}
			if strings.TrimSpace(input.Estado) != "" {
				filtered := make([]domain.Bus, 0, len(buses))
				for _, bus := range buses {
					if strings.EqualFold(bus.Estado, input.Estado) {
						filtered = append(filtered, bus)
					}
				}
				buses = filtered
			}
		}
		if strings.HasSuffix(request.Type, ".v1") {
			data = struct {
				Items []domain.Bus `json:"items"`
			}{Items: buses}
		} else {
			data = buses
		}
	case "bus.update.v1", "buses.bus.update", "bus.update":
		var input struct {
			BusID string `json:"busId"`
			domain.BusInput
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.UpdateBus(ctx, input.BusID, input.BusInput)
		}
	case "buses.bus.change-status", "bus.change-status":
		var input struct {
			BusID  string `json:"busId"`
			Estado string `json:"estado"`
			ID     string `json:"id"`
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			id := input.BusID
			if id == "" {
				id = input.ID
			}
			var bus domain.Bus
			statusCtx := domain.WithCorrelationID(ctx, request.CorrelationID)
			bus, err = h.service.GetBus(statusCtx, id)
			if err == nil {
				data, err = h.service.UpdateBus(statusCtx, id, domain.BusInput{
					DriverID: bus.DriverID,
					Patente:  bus.Patente,
					Empresa:  bus.Empresa,
					Estado:   input.Estado,
				})
			}
		}
	case "bus.delete.v1", "buses.bus.delete", "bus.delete":
		var input struct {
			BusID string `json:"busId"`
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			err = h.service.DeleteBus(ctx, input.BusID)
			if err == nil {
				data = map[string]any{"busId": input.BusID, "deleted": true}
			}
		}
	case "driver.create.v1", "buses.driver.create", "driver.create":
		var input domain.DriverInput
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.CreateDriver(ctx, input)
		}
	case "driver.get.v1", "buses.driver.get", "driver.get":
		var input struct {
			DriverID string `json:"driverId"`
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.GetDriver(ctx, input.DriverID)
		}
	case "driver.list.v1", "buses.driver.list", "driver.list":
		var drivers []domain.Driver
		drivers, err = h.service.ListDrivers(ctx)
		data = struct {
			Items []domain.Driver `json:"items"`
		}{Items: drivers}
	case "driver.update.v1", "buses.driver.update", "driver.update":
		var input struct {
			DriverID string `json:"driverId"`
			domain.DriverUpdateInput
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.UpdateDriver(ctx, input.DriverID, input.DriverUpdateInput)
		}
	case "driver.deactivate.v1", "buses.driver.deactivate", "driver.deactivate":
		var input struct {
			DriverID string `json:"driverId"`
		}
		err = decodeData(request.Data, &input)
		if err == nil {
			data, err = h.service.DeactivateDriver(ctx, input.DriverID)
		}
	default:
		err = errUnsupportedOperation
	}

	return commandResult{data: data, err: err}
}

func decodeData(raw json.RawMessage, target any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return domain.ErrInvalidInput
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("%w: %v", domain.ErrInvalidInput, err)
	}
	return nil
}

func newResponse(request requestEnvelope, result commandResult) (responseEnvelope, error) {
	messageID, err := newMessageID()
	if err != nil {
		return responseEnvelope{}, err
	}

	response := responseEnvelope{
		MessageID:     messageID,
		CorrelationID: request.CorrelationID,
		Type:          resultType(request.Type),
		SentAt:        time.Now().UTC().Format(time.RFC3339Nano),
		Data:          result.data,
	}
	if result.err != nil {
		ok := false
		response.Ok = &ok
		response.Data = nil
		response.Error = commandError(result.err, request.Type)
	} else {
		ok := true
		response.Ok = &ok
	}
	return response, nil
}

func resultType(requestType string) string {
	if requestType == "" {
		return "command.error.v1"
	}
	return strings.TrimSuffix(requestType, ".v1") + ".result.v1"
}

func commandError(err error, requestType string) *responseError {
	unified := !strings.Contains(requestType, ".v1")
	switch {
	case errors.Is(err, domain.ErrInvalidID), errors.Is(err, domain.ErrInvalidInput):
		if unified {
			return &responseError{Code: "BAD_REQUEST", Message: "Solicitud invalida"}
		}
		return &responseError{Code: "INVALID_REQUEST", Message: "Solicitud invalida"}
	case errors.Is(err, domain.ErrNotFound):
		return &responseError{Code: "NOT_FOUND", Message: "Recurso no encontrado"}
	case errors.Is(err, domain.ErrDriverNotFound):
		return &responseError{Code: "DRIVER_NOT_FOUND", Message: "Conductor no encontrado"}
	case errors.Is(err, domain.ErrPatenteAlreadyExists):
		if unified {
			return &responseError{Code: "CONFLICT", Message: "La patente ya existe"}
		}
		return &responseError{Code: "PATENTE_ALREADY_EXISTS", Message: "La patente ya existe"}
	case errors.Is(err, domain.ErrRUTAlreadyExists):
		if unified {
			return &responseError{Code: "CONFLICT", Message: "El RUT ya existe"}
		}
		return &responseError{Code: "RUT_ALREADY_EXISTS", Message: "El RUT ya existe"}
	case errors.Is(err, errUnsupportedOperation):
		return &responseError{Code: "UNSUPPORTED_OPERATION", Message: "Operacion no soportada"}
	default:
		slog.Error("Kafka request failed", "type", requestType, "err", err)
		if unified {
			return &responseError{Code: "INTERNAL", Message: "Error interno del servidor"}
		}
		return &responseError{Code: "INTERNAL_ERROR", Message: "Error interno"}
	}
}

func newMessageID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate message ID: %w", err)
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]), nil
}

func headerValue(headers []kafka.Header, name string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, name) {
			return strings.TrimSpace(string(header.Value))
		}
	}
	return ""
}
