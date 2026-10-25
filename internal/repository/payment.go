package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/codes"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type PaymentRepository struct {
	db *sql.DB
}

func (r *PaymentRepository) Save(ctx context.Context, payment *model.Payment) error {
	const op = "PaymentRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const paymentsQuery = `
	INSERT INTO payments (id, amount, currency, provider, status, metadata, idempotency_key)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (id) DO UPDATE
	SET
		amount = EXCLUDED.amount,
		currency = EXCLUDED.currency,
		provider = EXCLUDED.provider,
		status = EXCLUDED.status,
		metadata = EXCLUDED.metadata,
		idempotency_key = EXCLUDED.idempotency_key
	`

	_, err := r.db.ExecContext(
		ctx,
		paymentsQuery,
		payment.ID,
		payment.Amount,
		payment.Currency,
		payment.Provider,
		payment.Status,
		payment.Metadata,
		payment.IdempotencyKey,
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("%s: insert payment: %w", op, err)
	}

	return nil
}

func (r *PaymentRepository) GetByID(ctx context.Context, id string) (*model.Payment, error) {
	const op = "PaymentRepository.GetByID"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const query = `
	SELECT id, amount, currency, provider, status, metadata, created_at, updated_at, idempotency_key
	FROM payments
	WHERE id = $1
	`

	row := r.db.QueryRowContext(ctx, query, id)

	var payment model.Payment
	var metadata []byte

	err := row.Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Currency,
		&payment.Provider,
		&payment.Status,
		&metadata,
		&payment.CreatedAt,
		&payment.UpdatedAt,
		&payment.IdempotencyKey,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: payment not found: %w", op, ErrNotFound)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: payment query: %w", op, err)
	}

	if metadata != nil {
		payment.Metadata = new(json.RawMessage)
		*payment.Metadata = json.RawMessage(metadata)
	}

	return &payment, nil
}

func (r *PaymentRepository) GetByIdempotencyKey(ctx context.Context, key string) (*model.Payment, error) {
	const op = "PaymentRepository.GetByIdempotencyKey"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const query = `
	SELECT id, amount, currency, provider, status, metadata, created_at, updated_at, idempotency_key
	FROM payments
	WHERE idempotency_key = $1
	`

	row := r.db.QueryRowContext(ctx, query, key)

	var payment model.Payment
	var metadata []byte

	err := row.Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Currency,
		&payment.Provider,
		&payment.Status,
		&metadata,
		&payment.CreatedAt,
		&payment.UpdatedAt,
		&payment.IdempotencyKey,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: payment not found: %w", op, ErrNotFound)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: payment query: %w", op, err)
	}

	if metadata != nil {
		payment.Metadata = new(json.RawMessage)
		*payment.Metadata = json.RawMessage(metadata)
	}

	return &payment, nil
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}
