package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type EventRepository struct {
	db *sql.DB
}

func (r *EventRepository) Save(ctx context.Context, event *model.Event) error {
	const op = "EventRepository.Save"

	const query = `
	INSERT INTO payment_events (id, event_type, payment_id, payload, created_at)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT DO UPDATE 
	SET
		event_type = EXCLUDED.event_type,
		payment_id = EXCLUDED.payment_id,
		payload = EXCLUDED.payload,
		created_at = EXCLUDED.created_at;
	`

	_, err := r.db.ExecContext(
		ctx,
		query,
		event.EventType,
		event.PaymentID,
		event.Payload,
	)

	if err != nil {
		return fmt.Errorf("%s: insert event: %w", op, err)
	}

	return nil
}

func NewEventRepository(db *sql.DB) *EventRepository {
	return &EventRepository{db: db}
}
