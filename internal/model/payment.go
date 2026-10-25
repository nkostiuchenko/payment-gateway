package model

import (
	"encoding/json"
	"time"
)

type Payment struct {
	ID             string           `json:"id"`
	Amount         int              `json:"amount"`
	Currency       string           `json:"currency"`
	Provider       string           `json:"provider"`
	Status         string           `json:"status"`
	Metadata       *json.RawMessage `json:"metadata"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
	IdempotencyKey string           `json:"idempotency_key"`
}
