package repository

import (
	"context"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/common"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type EventRepository struct {
	db QueryerExecer
}

func (r *EventRepository) Save(ctx context.Context, event *model.Event) error {
	const op = "EventRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const eventQuery = `
	INSERT INTO payment_events (id, event_type, payment_id, payload, created_at)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (id) DO UPDATE 
	SET
		event_type = EXCLUDED.event_type,
		payment_id = EXCLUDED.payment_id,
		payload = EXCLUDED.payload,
		created_at = EXCLUDED.created_at;
	`

	_, err := r.db.ExecContext(
		ctx,
		eventQuery,
		event.ID,
		event.EventType,
		event.PaymentID,
		event.Payload,
		event.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("%s: insert event: %w", op, err)
	}

	const outboxQuery = `
	INSERT INTO outbox (id, topic, message)
	VALUES ($1, $2, $3)
	`

	_, err = r.db.ExecContext(
		ctx,
		outboxQuery,
		event.ID,
		common.EventsTopic,
		event.Payload,
	)

	if err != nil {
		return fmt.Errorf("%s: insert outbox: %w", op, err)
	}

	return nil
}

func NewEventRepository(db QueryerExecer) *EventRepository {
	return &EventRepository{db: db}
}
