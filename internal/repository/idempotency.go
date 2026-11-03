package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type IdempotencyKeysRepository struct {
	db QueryerExecer
}

func (r *IdempotencyKeysRepository) Register(ctx context.Context, key *model.IdempotencyKey) (paymentID string, created bool, err error) {
	const op = "IdempotencyKeysRepository.Register"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const insertQuery = `
	INSERT INTO idempotency_keys (id, payment_id)
	VALUES ($1, $2)
	ON CONFLICT (id) DO NOTHING
	RETURNING payment_id;
	`

	row := r.db.QueryRowContext(ctx, insertQuery, key.ID, key.PaymentID)

	err = row.Scan(&paymentID)

	if err == nil {
		return paymentID, true, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, fmt.Errorf("%s: register idempotency key: %w", op, err)
	}

	const selectQuery = `
	SELECT payment_id
	FROM idempotency_keys
	WHERE id = $1;
    `

	row = r.db.QueryRowContext(ctx, selectQuery, key.ID)

	err = row.Scan(&paymentID)

	if err != nil {
		return "", false, fmt.Errorf("%s: get existing idempotency key: %w", op, err)
	}

	return paymentID, false, nil
}

func NewIdempotencyKeysRepository(db QueryerExecer) *IdempotencyKeysRepository {
	return &IdempotencyKeysRepository{db: db}
}
