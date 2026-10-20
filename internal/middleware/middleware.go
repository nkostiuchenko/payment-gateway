package middleware

import (
	"log/slog"
	"net/http"
	"time"
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
			logger.Info("request", "method", r.Method, "url", r.URL.Path, "status", tw.Status, "duration", duration)
		})
	}
}
