package model

import "time"

type Payment struct {
	ID        string    `json:"id"`
	Amount    float64   `json:"amount"`
	Provider  string    `json:"provider"`
	Status    string    `json:"status"`
	Metadata  any       `json:"metadata"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
