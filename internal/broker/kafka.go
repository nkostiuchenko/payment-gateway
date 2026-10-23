package broker

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Kafka struct {
	producer sarama.SyncProducer
}

func (k *Kafka) Publish(ctx context.Context, topic string, content []byte) error {
	const op = "Kafka.Publish"

	ctx, span := telemetry.Tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(content),
	}

	otel.GetTextMapPropagator().Inject(ctx, telemetry.ProducerCarrier{Message: message})

	_, _, err := k.producer.SendMessage(message)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func NewKafkaBroker(address string) (*Kafka, error) {
	const op = "NewKafkaBroker"

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{address}, config)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Kafka{producer: producer}, nil
}
