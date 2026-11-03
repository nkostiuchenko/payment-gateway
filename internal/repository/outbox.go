package repository

import (
	"context"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type OutboxRepository struct {
	db QueryerExecer
}

func (r *OutboxRepository) Save(ctx context.Context, outbox *model.Outbox) error {
	const op = "OutboxRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const query = `
	INSERT INTO outbox (id, topic, message)
	VALUES ($1, $2, $3)
	`

	_, err := r.db.ExecContext(
		ctx,
		query,
		outbox.ID,
		outbox.Topic,
		outbox.Message,
	)

	if err != nil {
		return fmt.Errorf("%s: insert outbox: %w", op, err)
	}

	return nil
}

func NewOutboxRepository(db QueryerExecer) *OutboxRepository {
	return &OutboxRepository{db: db}
}
