package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/XSAM/otelsql"
)

func NewPostgres(dsn string) (*sql.DB, error) {
	db, err := otelsql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("db connection failed: %w", err)
	}

	// Maximum number of open connections.
	db.SetMaxOpenConns(25)

	// Maximum number of idle connections kept in the pool.
	db.SetMaxIdleConns(10)

	// Recycle idle connections after 5 minutes.
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Recycle connections after 30 minutes.
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
