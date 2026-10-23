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

var tracer = otel.Tracer("github.com/nkostiuchenko/payment-gateway/internal/broker")

func Publish[T any](ctx context.Context, topic string, producer sarama.SyncProducer, message T) error {
	const op = "Publish"

	ctx, span := tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	js, err := json.Marshal(message)
	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(js),
	}

	otel.GetTextMapPropagator().Inject(ctx, telemetry.ProducerCarrier{Message: msg})

	_, _, err = producer.SendMessage(msg)

	if err != nil {
		err = fmt.Errorf("%s: %w", op, err)
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return err
	}

	return nil
}
