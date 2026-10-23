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
}

type Broker interface {
	Publish(ctx context.Context, topic string, content []byte) error
}

type PaymentService struct {
	repo   PaymentRepository
	broker Broker
}

type CreatePaymentInput struct {
	Amount   int
	Currency string
	Provider string
	Metadata *json.RawMessage
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

	payment := model.Payment{
		ID:        uuid.NewString(),
		Amount:    input.Amount,
		Currency:  input.Currency,
		Provider:  input.Provider,
		Status:    "NEW",
		Metadata:  input.Metadata,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	command := model.CommandEnvelope[model.Payment]{
		CommandType:   "CreatePayment",
		SchemaVersion: 1,
		Payload:       payment,
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

	return &payment, nil
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

func NewPaymentService(repo PaymentRepository, broker Broker) PaymentService {
	return PaymentService{repo: repo, broker: broker}
}
