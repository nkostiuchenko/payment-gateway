package model

import "time"

type Command struct {
	ID          string    `json:"id"`
	CommandType string    `json:"command_type"`
	Payload     any       `json:"payload"`
	CreatedAt   time.Time `json:"created_at"`
}
