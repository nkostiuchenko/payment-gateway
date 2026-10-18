package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/XSAM/otelsql"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nkostiuchenko/payment-gateway/internal/config"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"github.com/nkostiuchenko/payment-gateway/internal/provider"
	"github.com/nkostiuchenko/payment-gateway/internal/repository"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
)

type Provider interface {
	Pay(ctx context.Context, amount int, currency string) error
}

const (
	commandsTopic = "payment.commands"
	eventsTopic   = "payment.events"
	address       = "localhost:9092"
)

var (
	ErrUnknownPaymentProvider = errors.New("unknown payment provider")
)

var (
	paymentProviders = map[string]Provider{
		"stripe": provider.NewStripeProvider(),
	}
)

type EventEnvelope[T any] struct {
	EventType     string `json:"event_type"`
	SchemaVersion int    `json:"schema_version"`
	Payload       T      `json:"payload"`
}

type PaymentProcessingStarted struct {
	PaymentID string `json:"payment_id"`
}

type PaymentProcessingFailed struct {
	PaymentID string `json:"payment_id"`
}

type PaymentProcessingSucceeded struct {
	PaymentID string `json:"payment_id"`
}

type PaymentCreated struct {
	model.Payment
}

func Publish[T any](producer sarama.SyncProducer, envelope EventEnvelope[T]) error {
	const op = "Publish"

	js, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: eventsTopic,
		Value: sarama.ByteEncoder(js),
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func PublishProcessingStarted(producer sarama.SyncProducer, paymentID string) error {
	const op = "PublishProcessingStarted"

	err := Publish(producer, EventEnvelope[PaymentProcessingStarted]{
		EventType:     "ProcessingStarted",
		SchemaVersion: 1,
		Payload:       PaymentProcessingStarted{PaymentID: paymentID},
	})

	return fmt.Errorf("%s: %w", op, err)
}

func PublishProcessingFailed(producer sarama.SyncProducer, paymentID string) error {
	const op = "PublishProcessingFailed"

	err := Publish(producer, EventEnvelope[PaymentProcessingStarted]{
		EventType:     "ProcessingFailed",
		SchemaVersion: 1,
		Payload:       PaymentProcessingStarted{PaymentID: paymentID},
	})

	return fmt.Errorf("%s: %w", op, err)
}

func PublishProcessingSucceeded(producer sarama.SyncProducer, paymentID string) error {
	const op = "PublishProcessingSucceeded"

	err := Publish(producer, EventEnvelope[PaymentProcessingSucceeded]{
		EventType:     "ProcessingSucceeded",
		SchemaVersion: 1,
		Payload:       PaymentProcessingSucceeded{PaymentID: paymentID},
	})

	return fmt.Errorf("%s: %w", op, err)
}

func PublishPaymentCreated(producer sarama.SyncProducer, payment model.Payment) error {
	const op = "PublishPaymentCreated"

	err := Publish(producer, EventEnvelope[model.Payment]{
		EventType:     "PaymentCreated",
		SchemaVersion: 1,
		Payload:       payment,
	})

	return fmt.Errorf("%s: %w", op, err)
}

type CommandEnvelope[T any] struct {
	CommandType   string `json:"command_type"`
	SchemaVersion int    `json:"schema_version"`
	Payload       T      `json:"payload"`
}

func ProcessPayment(producer sarama.SyncProducer, payment model.Payment) error {
	const op = "ProcessPayment"

	PublishProcessingStarted(producer, payment.ID)

	provider, ok := paymentProviders[payment.Provider]
	if !ok {
		PublishProcessingFailed(producer, payment.ID)
		return fmt.Errorf("%s: %w", op, ErrUnknownPaymentProvider)
	}

	err := provider.Pay(context.Background(), payment.Amount, payment.Currency)
	if err != nil {
		PublishProcessingFailed(producer, payment.ID)
		return fmt.Errorf("%s: %w", op, err)
	}

	PublishProcessingSucceeded(producer, payment.ID)

	return nil
}

type Worker struct {
	logger            *slog.Logger
	paymentRepository *repository.PaymentRepository
	eventRepository   *repository.EventRepository
	commandRepository *repository.CommandRepository
	producer          sarama.SyncProducer
	wg                *sync.WaitGroup
}

func (w *Worker) HandleCommandMessage(message *sarama.ConsumerMessage) {
	var command CommandEnvelope[json.RawMessage]
	if err := json.Unmarshal(message.Value, &command); err != nil {
		w.logger.Error("malformed command", "error", err)
		return
	}

	if !slices.Contains([]string{"CreatePayment"}, command.CommandType) {
		w.logger.Error("unknown command type", "command_type", command.CommandType)
		return
	}

	if command.CommandType == "CreatePayment" {
		var payment model.Payment
		if err := json.Unmarshal(command.Payload, &payment); err != nil {
			w.logger.Error("malformed command payload", "error", err)
			return
		}

		w.commandRepository.Save(context.Background(), &model.Command{
			ID:          uuid.NewString(),
			CommandType: "CreatePayment",
			Payload:     payment,
			CreatedAt:   time.Now(),
		})

		PublishPaymentCreated(w.producer, payment)
	}
}

func (w *Worker) HandleEventMessage(message *sarama.ConsumerMessage) {
	var envelope EventEnvelope[json.RawMessage]
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		w.logger.Error("malformed event", "error", err)
		return
	}

	if !slices.Contains([]string{"PaymentCreated"}, envelope.EventType) {
		w.logger.Error("unknown event type", "command_type", envelope.EventType)
		return
	}

	if envelope.EventType == "PaymentCreated" {
		var payment model.Payment
		if err := json.Unmarshal(envelope.Payload, &payment); err != nil {
			w.logger.Error("malformed event payload", "error", err)
			return
		}

		// FIXME: no exception handling
		w.paymentRepository.Save(context.Background(), &payment)

		// FIXME: no exception handling
		w.eventRepository.Save(context.Background(), &model.Event{
			ID:        uuid.NewString(),
			EventType: envelope.EventType,
			PaymentID: payment.ID,
			Payload:   envelope.Payload,
			CreatedAt: time.Now(),
		})

		// FIXME: one save may commit but another failed
		return
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	logger.Info("assign signal handler")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

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
	commandRepository := repository.NewCommandRepository(db)
	eventRepository := repository.NewEventRepository(db)

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

	commandConsumer, err := consumer.ConsumePartition(commandsTopic, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("kafka command consumer creation failed: %w", err)
	}

	eventConsumer, err := consumer.ConsumePartition(eventsTopic, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("kafka event consumer creation failed: %w", err)
	}

	worker := Worker{
		wg:                &sync.WaitGroup{},
		logger:            logger,
		producer:          producer,
		paymentRepository: paymentRepository,
		commandRepository: commandRepository,
		eventRepository:   eventRepository,
	}

loop:
	for {
		select {
		case <-sig:
			break loop
		case message := <-commandConsumer.Messages():
			worker.wg.Go(func() {
				worker.HandleCommandMessage(message)
			})
		case message := <-eventConsumer.Messages():
			worker.wg.Go(func() {
				worker.HandleEventMessage(message)
			})
		}
	}

	logger.Info("waiting for all messages be handled")
	worker.wg.Wait()

	logger.Info("closing event consumer")
	if err := eventConsumer.Close(); err != nil {
		return fmt.Errorf("event consumer close failed: %w", err)
	}

	logger.Info("closing command consumer")
	if err := commandConsumer.Close(); err != nil {
		return fmt.Errorf("command consumer close failed: %w", err)
	}

	logger.Info("closing consumer")
	if err := consumer.Close(); err != nil {
		return fmt.Errorf("consumer close failed: %w", err)
	}

	logger.Info("closing producer")
	if err := producer.Close(); err != nil {
		return fmt.Errorf("producer close failed: %w", err)
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
