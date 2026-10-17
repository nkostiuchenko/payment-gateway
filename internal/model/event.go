package model

import "time"

type Event struct {
	ID        string    `json:"id"`
	EventType string    `json:"event_type"`
	PaymentID string    `json:"payment_id"`
	Payload   any       `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}
