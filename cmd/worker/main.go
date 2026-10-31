package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/nkostiuchenko/payment-gateway/internal/broker"
	"github.com/nkostiuchenko/payment-gateway/internal/common"
	"github.com/nkostiuchenko/payment-gateway/internal/config"
	"github.com/nkostiuchenko/payment-gateway/internal/db"
	"github.com/nkostiuchenko/payment-gateway/internal/logging"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"github.com/nkostiuchenko/payment-gateway/internal/provider"
	"github.com/nkostiuchenko/payment-gateway/internal/repository"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
)

type Provider interface {
	Pay(ctx context.Context, amount int, currency string, webhook string) error
}

var (
	ErrUnknownPaymentProvider = errors.New("unknown payment provider")
)

var tracer = otel.Tracer("github.com/nkostiuchenko/payment-gateway/cmd/worker")

func PublishProcessingStarted(ctx context.Context, producer sarama.SyncProducer, paymentID string) error {
	const op = "PublishProcessingStarted"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	err := broker.Publish(ctx, common.EventsTopic, producer, model.EventEnvelope[model.PaymentProcessingStarted]{
		EventType:     common.EventProcessingStarted,
		SchemaVersion: 1,
		Payload:       model.PaymentProcessingStarted{PaymentID: paymentID},
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func PublishProcessingFailed(ctx context.Context, producer sarama.SyncProducer, paymentID string) error {
	const op = "PublishProcessingFailed"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	err := broker.Publish(ctx, common.EventsTopic, producer, model.EventEnvelope[model.PaymentProcessingStarted]{
		EventType:     common.EventProcessingFailed,
		SchemaVersion: 1,
		Payload:       model.PaymentProcessingStarted{PaymentID: paymentID},
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func PublishProcessingSucceeded(ctx context.Context, producer sarama.SyncProducer, paymentID string) error {
	const op = "PublishProcessingSucceeded"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	err := broker.Publish(ctx, common.EventsTopic, producer, model.EventEnvelope[model.PaymentProcessingSucceeded]{
		EventType:     common.EventProcessingSucceeded,
		SchemaVersion: 1,
		Payload:       model.PaymentProcessingSucceeded{PaymentID: paymentID},
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func PublishPaymentCreated(ctx context.Context, producer sarama.SyncProducer, payment model.Payment) error {
	const op = "PublishPaymentCreated"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	err := broker.Publish(ctx, common.EventsTopic, producer, model.EventEnvelope[model.Payment]{
		EventType:     common.EventPaymentCreated,
		SchemaVersion: 1,
		Payload:       payment,
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (w *Worker) ProcessPayment(ctx context.Context, producer sarama.SyncProducer, payment model.Payment) error {
	const op = "ProcessPayment"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	PublishProcessingStarted(ctx, producer, payment.ID)

	provider, ok := w.paymentProviders[payment.Provider]
	if !ok {
		PublishProcessingFailed(ctx, producer, payment.ID)
		return fmt.Errorf("%s: %w", op, ErrUnknownPaymentProvider)
	}

	webhook := w.cfg.WebhookURL + "/" + payment.ID

	err := provider.Pay(ctx, payment.Amount, payment.Currency, webhook)
	if err != nil {
		PublishProcessingFailed(ctx, producer, payment.ID)
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func Handle_CommandCreatePayment(ctx context.Context, w *Worker, command model.CommandEnvelope[json.RawMessage]) {
	const op = "Handle_CommandCreatePayment"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	var payment model.Payment
	if err := json.Unmarshal(command.Payload, &payment); err != nil {
		w.logger.Error("malformed command payload", "error", err)
		broker.Publish(ctx, common.DLQTopic, w.producer, command)
		return
	}

	logger := w.logger.With("payment_id", payment.ID, "command_type", command.CommandType)

	if err := w.commandRepository.Save(ctx, &model.Command{
		ID:          uuid.NewString(),
		CommandType: common.CommandCreatePayment,
		Payload:     payment,
		CreatedAt:   time.Now(),
	}); err != nil {
		logger.Error("command save failed", "error", err)
		return
	}

	PublishPaymentCreated(ctx, w.producer, payment)

	w.wg.Go(func() {
		w.ProcessPayment(ctx, w.producer, payment)
	})
}

func Handle_EventPaymentCreated(ctx context.Context, w *Worker, event model.EventEnvelope[json.RawMessage]) {
	const op = "Handle_EventPaymentCreated"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	var payment model.Payment
	if err := json.Unmarshal(event.Payload, &payment); err != nil {
		w.logger.Error("malformed event payload", "error", err)
		broker.Publish(ctx, common.DLQTopic, w.producer, event)
		return
	}

	logger := w.logger.With("payment_id", payment.ID, "event_type", event.EventType)

	if err := w.eventRepository.Save(ctx, &model.Event{
		ID:        uuid.NewString(),
		EventType: event.EventType,
		PaymentID: payment.ID,
		Payload:   event.Payload,
		CreatedAt: time.Now(),
	}); err != nil {
		logger.Error("event save failed", "error", err)
		return
	}

	if err := w.paymentRepository.Save(ctx, &payment); err != nil {
		logger.Error("payment save failed", "error", err)
		return
	}
}

func Handle_EventPaymentProcessingStarted(ctx context.Context, w *Worker, event model.EventEnvelope[json.RawMessage]) {
	const op = "Handle_EventPaymentProcessingStarted"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	var payload model.PaymentProcessingStarted
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		w.logger.Error("malformed event payload", "error", err)
		broker.Publish(ctx, common.DLQTopic, w.producer, event)
		return
	}

	logger := w.logger.With("payment_id", payload.PaymentID, "event_type", event.EventType)

	payment, err := w.paymentRepository.GetByID(ctx, payload.PaymentID)
	if err != nil {
		logger.Error("payment not found", "error", err)
		return
	}

	payment.Status = common.StatusProcessing

	if err := w.eventRepository.Save(ctx, &model.Event{
		ID:        uuid.NewString(),
		EventType: event.EventType,
		PaymentID: payment.ID,
		Payload:   event.Payload,
		CreatedAt: time.Now(),
	}); err != nil {
		logger.Error("event save failed", "error", err)
		return
	}

	if err := w.paymentRepository.Save(ctx, payment); err != nil {
		logger.Error("payment save failed", "error", err)
		return
	}

}

func Handle_EventProcessingFailed(ctx context.Context, w *Worker, event model.EventEnvelope[json.RawMessage]) {
	const op = "Handle_EventProcessingFailed"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	var payload model.PaymentProcessingFailed
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		w.logger.Error("malformed event payload", "error", err)
		broker.Publish(ctx, common.DLQTopic, w.producer, event)
		return
	}

	logger := w.logger.With("payment_id", payload.PaymentID, "event_type", event.EventType)

	payment, err := w.paymentRepository.GetByID(ctx, payload.PaymentID)
	if err != nil {
		logger.Error("payment not found", "error", err)
		return
	}

	payment.Status = common.StatusFailed

	if err := w.eventRepository.Save(ctx, &model.Event{
		ID:        uuid.NewString(),
		EventType: event.EventType,
		PaymentID: payment.ID,
		Payload:   event.Payload,
		CreatedAt: time.Now(),
	}); err != nil {
		logger.Error("event save failed", "error", err)
		return
	}

	if err := w.paymentRepository.Save(ctx, payment); err != nil {
		logger.Error("payment save failed", "error", err)
		return
	}
}

func Handle_EventProcessingSucceeded(ctx context.Context, w *Worker, event model.EventEnvelope[json.RawMessage]) {
	const op = "Handle_EventProcessingFailed"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	var payload model.PaymentProcessingSucceeded
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		w.logger.Error("malformed event payload", "error", err)
		broker.Publish(ctx, common.DLQTopic, w.producer, event)
		return
	}

	logger := w.logger.With("payment_id", payload.PaymentID, "event_type", event.EventType)

	payment, err := w.paymentRepository.GetByID(ctx, payload.PaymentID)
	if err != nil {
		logger.Error("payment not found", "error", err)
		return
	}

	payment.Status = common.StatusCompleted

	if err := w.eventRepository.Save(ctx, &model.Event{
		ID:        uuid.NewString(),
		EventType: event.EventType,
		PaymentID: payment.ID,
		Payload:   event.Payload,
		CreatedAt: time.Now(),
	}); err != nil {
		logger.Error("event save failed", "error", err)
		return
	}

	if err := w.paymentRepository.Save(ctx, payment); err != nil {
		logger.Error("payment save failed", "error", err)
		return
	}
}

type Worker struct {
	cfg               config.Config
	logger            *slog.Logger
	paymentRepository *repository.PaymentRepository
	eventRepository   *repository.EventRepository
	commandRepository *repository.CommandRepository
	paymentProviders  map[string]Provider
	producer          sarama.SyncProducer
	wg                *sync.WaitGroup
}

func (w *Worker) HandleCommandMessage(message *sarama.ConsumerMessage) {
	const op = "Worker.HandleCommandMessage"

	ctx := otel.GetTextMapPropagator().Extract(context.Background(), telemetry.ConsumerCarrier{Message: message})

	ctx, span := tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var command model.CommandEnvelope[json.RawMessage]
	if err := json.Unmarshal(message.Value, &command); err != nil {
		w.logger.Error("malformed command", "error", err)
		broker.PublishRaw(ctx, common.DLQTopic, w.producer, message.Value)
		return
	}

	switch command.CommandType {
	case common.CommandCreatePayment:
		Handle_CommandCreatePayment(ctx, w, command)
	default:
		w.logger.Error("unknown command type", "command_type", command.CommandType)
		broker.PublishRaw(ctx, common.DLQTopic, w.producer, message.Value)
	}
}

func (w *Worker) HandleEventMessage(message *sarama.ConsumerMessage) {
	const op = "Worker.HandleEventMessage"

	ctx := otel.GetTextMapPropagator().Extract(context.Background(), telemetry.ConsumerCarrier{Message: message})

	ctx, span := tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var event model.EventEnvelope[json.RawMessage]
	if err := json.Unmarshal(message.Value, &event); err != nil {
		w.logger.Error("malformed event", "error", err)
		broker.PublishRaw(ctx, common.DLQTopic, w.producer, message.Value)
		return
	}

	switch event.EventType {
	case common.EventPaymentCreated:
		Handle_EventPaymentCreated(ctx, w, event)
	case common.EventProcessingStarted:
		Handle_EventPaymentProcessingStarted(ctx, w, event)
	case common.EventProcessingFailed:
		Handle_EventProcessingFailed(ctx, w, event)
	case common.EventProcessingSucceeded:
		Handle_EventProcessingSucceeded(ctx, w, event)
	default:
		w.logger.Error("unknown event type", "event_type", event.EventType)
		broker.PublishRaw(ctx, common.DLQTopic, w.producer, message.Value)
	}

}

func run() error {
	logger := slog.New(logging.NewOtelID(slog.NewJSONHandler(os.Stdout, nil)))

	logger.Info("assign signal handler")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed load config: %w", err)
	}

	shutdownTracing, err := telemetry.InitTracing(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("otel setup failed: %w", err)
	}

	pgDB, err := db.NewPostgres(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres connection failed: %w", err)
	}

	if err := pgDB.Ping(); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	mongoDB, err := db.NewMongo(cfg.MongoUsername, cfg.MongoPassword, cfg.MongoDB)
	if err != nil {
		return fmt.Errorf("mongo connection failed")
	}

	if err := mongoDB.Client().Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("mongo ping failed: %w", err)
	}

	paymentsCollection := mongoDB.Collection(common.MongoPaymentsCollection)

	paymentRepository := repository.NewPaymentRepository(paymentsCollection)
	commandRepository := repository.NewCommandRepository(pgDB)
	eventRepository := repository.NewEventRepository(pgDB)

	paymentProviders := map[string]Provider{"stripe": provider.NewStripeProvider(cfg)}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{cfg.KafkaURL}, config)
	if err != nil {
		return fmt.Errorf("kafka producer creation failed: %w", err)
	}

	consumer, err := sarama.NewConsumer([]string{cfg.KafkaURL}, config)
	if err != nil {
		return fmt.Errorf("kafka consumer creation failed: %w", err)
	}

	// FIXME: replace with consumer group
	commandConsumer, err := consumer.ConsumePartition(common.CommandsTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("kafka command consumer creation failed: %w", err)
	}

	// FIXME: replace with consumer group
	eventConsumer, err := consumer.ConsumePartition(common.EventsTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("kafka event consumer creation failed: %w", err)
	}

	worker := Worker{
		cfg:               cfg,
		wg:                &sync.WaitGroup{},
		logger:            logger,
		producer:          producer,
		paymentRepository: paymentRepository,
		commandRepository: commandRepository,
		eventRepository:   eventRepository,
		paymentProviders:  paymentProviders,
	}

loop:
	for {
		select {
		case <-sig:
			break loop
		case message := <-commandConsumer.Messages():
			worker.HandleCommandMessage(message)
		case message := <-eventConsumer.Messages():
			worker.HandleEventMessage(message)
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

	logger.Info("closing mongo connection")
	if err := mongoDB.Client().Disconnect(context.Background()); err != nil {
		return fmt.Errorf("mongo close failed: %w", err)
	}

	logger.Info("closing postgres connection")
	if err := pgDB.Close(); err != nil {
		return fmt.Errorf("postgres close failed: %w", err)
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
