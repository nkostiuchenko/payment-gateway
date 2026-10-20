package model

import "time"

type Command struct {
	ID          string    `json:"id"`
	CommandType string    `json:"command_type"`
	Payload     any       `json:"payload"`
	CreatedAt   time.Time `json:"created_at"`
}

type CommandEnvelope[T any] struct {
	CommandType   string `json:"command_type"`
	SchemaVersion int    `json:"schema_version"`
	Payload       T      `json:"payload"`
}
