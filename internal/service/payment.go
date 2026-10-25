package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/codes"

	"github.com/google/uuid"
	"github.com/nkostiuchenko/payment-gateway/internal/common"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type PaymentRepository interface {
	Save(ctx context.Context, payment *model.Payment) error
	GetByID(ctx context.Context, id string) (*model.Payment, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*model.Payment, error)
}

type Broker interface {
	Publish(ctx context.Context, topic string, content []byte) error
}

type PaymentService struct {
	repo   PaymentRepository
	broker Broker
}

type CreatePaymentInput struct {
	Amount         int
	Currency       string
	Provider       string
	Metadata       *json.RawMessage
	IdempotencyKey string
}

type WebhookInput struct {
	PaymentID string
	Status    string
	Intent    string
}

func (s *PaymentService) Create(ctx context.Context, input CreatePaymentInput) (*model.Payment, error) {
	const op = "PaymentService.Create"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	if input.Amount <= 0 {
		return nil, fmt.Errorf("%s: amount must be positive: %w", op, ErrInvalidPayment)
	}

	if strings.TrimSpace(input.Provider) == "" {
		return nil, fmt.Errorf("%s: provider required: %w", op, ErrInvalidPayment)
	}

	payment, err := s.repo.GetByIdempotencyKey(ctx, input.IdempotencyKey)
	if err == nil {
		return payment, nil
	}

	payment = &model.Payment{
		ID:             uuid.NewString(),
		Amount:         input.Amount,
		Currency:       input.Currency,
		Provider:       input.Provider,
		Status:         "NEW",
		Metadata:       input.Metadata,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		IdempotencyKey: input.IdempotencyKey,
	}

	command := model.CommandEnvelope[model.Payment]{
		CommandType:   "CreatePayment",
		SchemaVersion: 1,
		Payload:       *payment,
	}

	js, err := json.Marshal(command)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("%s: marshaling of command: %w", op, err)
	}

	if err := s.broker.Publish(ctx, common.CommandsTopic, js); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("%s: publish create payment: %w", op, err)
	}

	return payment, nil
}

func (s *PaymentService) GetByID(ctx context.Context, id string) (*model.Payment, error) {
	const op = "PaymentService.GetByID"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	if _, err := uuid.Parse(id); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("%s: %w", op, ErrInvalidId)
	}

	payment, err := s.repo.GetByID(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return payment, nil
}

func (s *PaymentService) Webhook(ctx context.Context, input WebhookInput) error {
	const op = "PaymentService.Webhook"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	if _, err := uuid.Parse(input.PaymentID); err != nil {
		err = fmt.Errorf("%s: %w", op, ErrInvalidId)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	var event any
	switch input.Status {
	case common.WebhookStatusSuccess:
		event = model.EventEnvelope[model.PaymentProcessingSucceeded]{
			EventType:     common.EventProcessingSucceeded,
			SchemaVersion: 1,
			Payload:       model.PaymentProcessingSucceeded{PaymentID: input.PaymentID},
		}
	case common.WebhookStatusError:
		event = model.EventEnvelope[model.PaymentProcessingFailed]{
			EventType:     common.EventProcessingFailed,
			SchemaVersion: 1,
			Payload:       model.PaymentProcessingFailed{PaymentID: input.PaymentID},
		}
	default:
		err := fmt.Errorf("%s: unknown status %q: %w", op, input.Status, ErrUnknownStatus)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	js, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("%s: marshaling of event: %w", op, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := s.broker.Publish(ctx, common.EventsTopic, js); err != nil {
		err = fmt.Errorf("%s: publish event failed: %w", op, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func NewPaymentService(repo PaymentRepository, broker Broker) PaymentService {
	return PaymentService{repo: repo, broker: broker}
}
