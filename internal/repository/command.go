package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type CommandRepository struct {
	db *sql.DB
}

func (r *CommandRepository) Save(ctx context.Context, command *model.Command) error {
	const op = "CommandRepository.Save"

	const query = `
	INSERT INTO processed_commands (id, command_type, payload, created_at)
	VALUES ($1, $2, $3, $4)
	ON CONFLICT (id) DO UPDATE 
	SET
		command_type = EXCLUDED.command_type,
		payload = EXCLUDED.payload,
		created_at = EXCLUDED.created_at;
	`

	_, err := r.db.ExecContext(
		ctx,
		query,
		command.ID,
		command.CommandType,
		command.Payload,
		command.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("%s: insert command: %w", op, err)
	}

	return nil
}

func NewCommandRepository(db *sql.DB) *CommandRepository {
	return &CommandRepository{db: db}
}
