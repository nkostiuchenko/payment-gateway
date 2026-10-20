package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/nkostiuchenko/payment-gateway/internal/common/requestid"
)

func ContentJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

type trackingWriter struct {
	http.ResponseWriter
	Status int
}

func (t *trackingWriter) WriteHeader(statusCode int) {
	t.Status = statusCode
	t.ResponseWriter.WriteHeader(statusCode)
}

func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			tw := &trackingWriter{ResponseWriter: w, Status: 200}
			next.ServeHTTP(tw, r)
			duration := time.Since(start)
			logger.InfoContext(r.Context(), "request", "method", r.Method, "url", r.URL.Path, "status", tw.Status, "duration", duration)
		})
	}
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestid.New()
		ctx := requestid.NewContext(r.Context(), id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
