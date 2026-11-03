package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type QueryerExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Repositories struct {
	Command        *CommandRepository
	Event          *EventRepository
	Outbox         *OutboxRepository
	IdempotencyKey *IdempotencyKeysRepository
}

type UnitOfWork struct {
	db      *sql.DB
	options *sql.TxOptions
}

func (u *UnitOfWork) WithinTx(ctx context.Context, fn func(ctx context.Context, repos Repositories) error) (err error) {
	const op = "UnitOfWork.WithinTx"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	tx, err := u.db.BeginTx(ctx, u.options)
	if err != nil {
		return fmt.Errorf("%s: begin transaction: %w", op, err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}

		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				err = errors.Join(err, fmt.Errorf("%s: rollback transaction: %w", op, rbErr))
			}
		}
	}()

	repos := Repositories{
		Command:        NewCommandRepository(tx),
		Event:          NewEventRepository(tx),
		Outbox:         NewOutboxRepository(tx),
		IdempotencyKey: NewIdempotencyKeysRepository(tx),
	}

	if err = fn(ctx, repos); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		// A commit error can be ambiguous: the driver/database may not be able
		// to tell whether the transaction committed. Surface it clearly.
		return fmt.Errorf("%s: commit transaction: %w", op, err)
	}

	return nil
}

func NewUnitOfWork(db *sql.DB, options *sql.TxOptions) *UnitOfWork {
	return &UnitOfWork{
		db:      db,
		options: options,
	}
}
