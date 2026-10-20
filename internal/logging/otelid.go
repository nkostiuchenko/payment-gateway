package logging

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type otelID struct {
	next slog.Handler
}

func (r otelID) Enabled(ctx context.Context, level slog.Level) bool {
	return r.next.Enabled(ctx, level)
}

func (r otelID) Handle(ctx context.Context, record slog.Record) error {
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", span.TraceID().String()),
			slog.String("span_id", span.SpanID().String()),
		)
	}
	return r.next.Handle(ctx, record)
}

func (r otelID) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &otelID{next: r.next.WithAttrs(attrs)}
}

func (r otelID) WithGroup(name string) slog.Handler {
	return &otelID{next: r.next.WithGroup(name)}
}

func NewOtelID(next slog.Handler) *otelID {
	return &otelID{next: next}
}
