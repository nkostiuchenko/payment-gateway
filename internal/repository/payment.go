package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type PaymentRepository struct {
	db *sql.DB
}

func (r *PaymentRepository) Create(ctx context.Context, payment *model.Payment) error {
	const op = "PaymentRepository.Create"

	status := "NEW"

	const paymentsQuery = `
	INSERT INTO payments (amount, currency, provider, status, metadata)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id;
	`

	row := r.db.QueryRowContext(
		ctx,
		paymentsQuery,
		payment.Amount,
		payment.Currency,
		payment.Provider,
		status,
		payment.Metadata,
	)

	var paymentID string
	if err := row.Scan(&paymentID); err != nil {
		return fmt.Errorf("%s: insert payment: %w", op, err)
	}

	payment.ID = paymentID
	payment.Status = status

	return nil
}

func (r *PaymentRepository) GetByID(ctx context.Context, id string) (*model.Payment, error) {
	const op = "PaymentRepository.GetByID"

	const query = `
	SELECT id, amount, provider, status, metadata, created_at, updated_at
	FROM payments
	WHERE id = $1
	`

	row := r.db.QueryRowContext(ctx, query, id)

	var payment model.Payment
	var metadata []byte

	err := row.Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Provider,
		&payment.Status,
		&metadata,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: payment not found: %w", op, ErrNotFound)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: payment query: %w", op, err)
	}

	if err := json.Unmarshal(metadata, &payment.Metadata); err != nil {
		return nil, fmt.Errorf("%s: unmarshaling metadata: %w", op, err)
	}

	return &payment, nil
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}
