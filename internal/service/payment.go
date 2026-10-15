package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type PaymentRepository interface {
	Create(ctx context.Context, payment *model.Payment) error
	GetByID(ctx context.Context, id string) (*model.Payment, error)
}

type PaymentService struct {
	repo PaymentRepository
}

type CreatePaymentInput struct {
	Amount   int
	Provider string
	Metadata any
}

func (s *PaymentService) Create(ctx context.Context, input CreatePaymentInput) (*model.Payment, error) {
	const op = "PaymentService.Create"

	if input.Amount <= 0 {
		return nil, fmt.Errorf("%s: amount must be positive: %w", op, ErrInvalidPayment)
	}

	if strings.TrimSpace(input.Provider) == "" {
		return nil, fmt.Errorf("%s: provider required: %w", op, ErrInvalidPayment)
	}

	payment := model.Payment{
		Amount:   input.Amount,
		Provider: input.Provider,
		Metadata: input.Metadata,
	}

	if err := s.repo.Create(ctx, &payment); err != nil {
		return nil, fmt.Errorf("%s: create payment: %w", op, err)
	}

	return &payment, nil
}

func (s *PaymentService) GetByID(ctx context.Context, id string) (*model.Payment, error) {
	const op = "PaymentService.GetByID"

	if _, err := uuid.Parse(id); err != nil {
		return nil, fmt.Errorf("%s: %w", op, ErrInvalidId)
	}

	payment, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return payment, nil
}

func NewPaymentService(repo PaymentRepository) PaymentService {
	return PaymentService{repo: repo}
}
