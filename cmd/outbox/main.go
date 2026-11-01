package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nkostiuchenko/payment-gateway/internal/config"
	"github.com/nkostiuchenko/payment-gateway/internal/db"
	"github.com/nkostiuchenko/payment-gateway/internal/logging"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
)

func ProcessRelay(producer sarama.SyncProducer, db *sql.DB) error {
	const op = "ProcessRelay"

	const selectQuery = `
		SELECT id, topic, message, state, created_at, processed_at
		FROM outbox
		WHERE state = 'pending'
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`

	const updateQuery = `
		UPDATE outbox
		SET state = 'processed', processed_at = now() 
		WHERE id = $1
	`

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	defer tx.Rollback()

	rows, err := tx.Query(selectQuery)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if !rows.Next() {
		rows.Close()
		return nil
	}

	var outbox model.Outbox

	err = rows.Scan(
		&outbox.ID,
		&outbox.Topic,
		&outbox.Message,
		&outbox.State,
		&outbox.CreatedAt,
		&outbox.ProcessedAt,
	)

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	rows.Close()

	js, err := json.Marshal(outbox.Message)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: outbox.Topic,
		Value: sarama.ByteEncoder(js),
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	_, err = tx.Exec(updateQuery, outbox.ID)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return tx.Commit()
}

func run() error {
	logger := slog.New(logging.NewOtelID(slog.NewJSONHandler(os.Stdout, nil)))

	logger.Info("assign signal handler")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	logger.Info("loading config")
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed load config: %w", err)
	}

	logger.Info("setting up tracing")
	shutdownTracing, err := telemetry.InitTracing(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("tracing setup failed: %w", err)
	}

	logger.Info("setting up postgres")
	pgDB, err := db.NewPostgres(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres connection failed: %w", err)
	}

	logger.Info("trying to ping postgres")
	if err := pgDB.Ping(); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	logger.Info("setting up kafka producer")
	producer, err := sarama.NewSyncProducer([]string{cfg.KafkaURL}, config)
	if err != nil {
		return fmt.Errorf("kafka producer creation failed: %w", err)
	}

	ticker := time.NewTicker(time.Duration(cfg.OutboxDelay) * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-sig:
			break loop
		case <-ticker.C:
			if err := ProcessRelay(producer, pgDB); err != nil {
				logger.Error(err.Error())
			}
		}
	}

	logger.Info("shutting down kafka producer")
	if err := producer.Close(); err != nil {
		return fmt.Errorf("kafka producer shutdown failed: %w", err)
	}

	logger.Info("shutting down postgres")
	if err := pgDB.Close(); err != nil {
		return fmt.Errorf("postgres shutdown failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Info("shutting down tracing")
	if err := shutdownTracing(ctx); err != nil {
		return fmt.Errorf("tracing shutdown failed: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("applicaiton failed", "error", err)
		os.Exit(1)
	}
}
