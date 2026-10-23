package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
)

func Publish[T any](ctx context.Context, topic string, producer sarama.SyncProducer, message T) error {
	const op = "Publish"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	js, err := json.Marshal(message)
	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return err
	}

	if err := PublishRaw(ctx, topic, producer, js); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func PublishRaw(ctx context.Context, topic string, producer sarama.SyncProducer, content []byte) error {
	const op = "PublishRaw"

	ctx, span := tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(content),
	}

	otel.GetTextMapPropagator().Inject(ctx, telemetry.ProducerCarrier{Message: msg})

	_, _, err := producer.SendMessage(msg)

	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return err
	}

	return nil
}
