package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/XSAM/otelsql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nkostiuchenko/payment-gateway/internal/config"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"github.com/nkostiuchenko/payment-gateway/internal/repository"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
)

type PaymentCreateCommand struct {
	Amount   int    `json:"amount"`
	Currency string `json:"currency"`
	Provider string `json:"provider"`
	Metadata any    `json:"metadata"`
}

const (
	commandsTopic = "payment.commands"
	address       = "localhost:9092"
)

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed load config: %w", err)
	}

	shutdownTracing, err := telemetry.InitTracing(context.Background())
	if err != nil {
		return fmt.Errorf("otel setup failed: %w", err)
	}

	db, err := otelsql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db connection failed: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping failed: %w", err)
	}

	paymentRepository := repository.NewPaymentRepository(db)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{address}, config)
	if err != nil {
		return fmt.Errorf("kafka producer creation failed: %w", err)
	}

	consumer, err := sarama.NewConsumer([]string{address}, config)
	if err != nil {
		return fmt.Errorf("kafka consumer creation failed: %w", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(commandsTopic, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("kafka consumer partition creation failed: %w", err)
	}

	for {
		message := <-partitionConsumer.Messages()

		var command PaymentCreateCommand
		if err := json.Unmarshal(message.Value, &command); err != nil {
			logger.Error("malformed command", "error", err)
			continue
		}

		payment := model.Payment{
			Amount:   command.Amount,
			Provider: command.Provider,
			Metadata: command.Metadata,
		}

		err := paymentRepository.Create(context.Background(), &payment)

		if err != nil {
			logger.Error("payment creation failed", "error", err)
			continue
		}

		js, err := json.Marshal(payment)
		if err != nil {
			logger.Error("failed to marshal payment", "error", err)
		}

		producer.SendMessage(&sarama.ProducerMessage{
			Topic: "payment.events",
			Value: sarama.ByteEncoder(js),
		})
	}

	logger.Info("closing db connection")
	if err := db.Close(); err != nil {
		return fmt.Errorf("db close failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Info("shutting down tracing")
	if err := shutdownTracing(ctx); err != nil {
		return fmt.Errorf("otel shutdown failed: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("applicaiton failed", "error", err)
		os.Exit(1)
	}
}
