package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	appName = "payment-service"
)

func InitTracing(ctx context.Context) (func(context.Context) error, error) {
	const op = "InitTracing"

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: create OTLP exporter: %w", op, err)
	}

	res, err := sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewSchemaless(
			attribute.String("service.name", appName),
			attribute.String("deployment.environment.name", "development"),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("%s: create resource: %w", op, err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return func(ctx context.Context) error { return tp.Shutdown(ctx) }, nil
}

func TracingShutdown(shutdown func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return shutdown(ctx)
}
