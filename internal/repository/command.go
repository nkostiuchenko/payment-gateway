package repository

import (
	"context"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/common"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type CommandRepository struct {
	db QueryerExecer
}

func (r *CommandRepository) Save(ctx context.Context, command *model.Command) error {
	const op = "CommandRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	const commandQuery = `
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
		commandQuery,
		command.ID,
		command.CommandType,
		command.Payload,
		command.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("%s: insert command: %w", op, err)
	}

	const outboxQuery = `
	INSERT INTO outbox (id, topic, message)
	VALUES ($1, $2, $3)
	`

	_, err = r.db.ExecContext(
		ctx,
		outboxQuery,
		command.ID,
		common.CommandsTopic,
		command.Payload,
	)

	if err != nil {
		return fmt.Errorf("%s: insert outbox: %w", op, err)
	}

	return nil
}

func NewCommandRepository(db QueryerExecer) *CommandRepository {
	return &CommandRepository{db: db}
}
