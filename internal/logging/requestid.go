package logging

import (
	"context"
	"log/slog"

	"github.com/nkostiuchenko/payment-gateway/internal/common/requestid"
)

type requestIDHandler struct {
	next slog.Handler
}

func (r requestIDHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return r.next.Enabled(ctx, level)
}

func (r requestIDHandler) Handle(ctx context.Context, record slog.Record) error {
	id, ok := requestid.FromContext(ctx)
	if ok {
		record.AddAttrs(slog.String("request_id", id))
	}
	return r.next.Handle(ctx, record)
}

func (r requestIDHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &requestIDHandler{next: r.next.WithAttrs(attrs)}
}

func (r requestIDHandler) WithGroup(name string) slog.Handler {
	return &requestIDHandler{next: r.next.WithGroup(name)}
}

func NewRequestIDHandler(next slog.Handler) *requestIDHandler {
	return &requestIDHandler{next: next}
}
