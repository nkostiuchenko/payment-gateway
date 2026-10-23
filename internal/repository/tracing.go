package repository

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("github.com/nkostiuchenko/payment-gateway/internal/repository")
