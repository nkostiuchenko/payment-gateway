package model

import "time"

type Event struct {
	ID        string    `json:"id"`
	EventType string    `json:"event_type"`
	PaymentID string    `json:"payment_id"`
	Payload   any       `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type EventEnvelope[T any] struct {
	EventType     string `json:"event_type"`
	SchemaVersion int    `json:"schema_version"`
	Payload       T      `json:"payload"`
}

type PaymentProcessingStarted struct {
	PaymentID string `json:"payment_id"`
}

type PaymentProcessingFailed struct {
	PaymentID string `json:"payment_id"`
}

type PaymentProcessingSucceeded struct {
	PaymentID string `json:"payment_id"`
}

type PaymentCreated struct {
	Payment
}
