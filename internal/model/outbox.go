package model

import (
	"encoding/json"
	"time"
)

type Outbox struct {
	ID          string           `json:"id"`
	Topic       string           `json:"topic"`
	Message     *json.RawMessage `json:"message"`
	State       string           `json:"state"`
	CreatedAt   *time.Time       `json:"created_at"`
	ProcessedAt *time.Time       `json:"processed_at"`
}
