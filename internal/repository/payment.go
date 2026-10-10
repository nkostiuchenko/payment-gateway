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

	payment.Status = "NEW"

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback()

	const paymentsQuery = `
	INSERT INTO payments (amount, provider, status, metadata)
	VALUES ($1, $2, $3, $4)
	RETURNING id;
	`

	row := tx.QueryRow(
		paymentsQuery,
		payment.Amount,
		payment.Provider,
		payment.Status,
		payment.Metadata,
	)

	var paymentID string
	if err := row.Scan(&paymentID); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	payment.ID = paymentID

	const eventsQuery = `
	INSERT INTO payment_events (payment_id, payload)
	VALUES ($1, $2)
	RETURNING id;
	`

	js, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	row = tx.QueryRow(eventsQuery, paymentID, js)

	var eventID string
	if err := row.Scan(&eventID); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

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
		return nil, fmt.Errorf("%s: %w", op, ErrNotFound)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if err := json.Unmarshal(metadata, &payment.Metadata); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &payment, nil
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}
