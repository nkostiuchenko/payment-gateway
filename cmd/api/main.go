package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/nkostiuchenko/payment-gateway/internal/broker"
	"github.com/nkostiuchenko/payment-gateway/internal/config"
	"github.com/nkostiuchenko/payment-gateway/internal/db"
	"github.com/nkostiuchenko/payment-gateway/internal/dto"
	"github.com/nkostiuchenko/payment-gateway/internal/logging"
	"github.com/nkostiuchenko/payment-gateway/internal/metrics"
	"github.com/nkostiuchenko/payment-gateway/internal/middleware"
	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"github.com/nkostiuchenko/payment-gateway/internal/repository"
	"github.com/nkostiuchenko/payment-gateway/internal/service"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"
)

type PaymentService interface {
	GetByID(ctx context.Context, id string) (*model.Payment, error)
	Create(ctx context.Context, input service.CreatePaymentInput) (*model.Payment, error)
	Webhook(ctx context.Context, id string) error
}

type Server struct {
	Logger         *slog.Logger
	paymentService PaymentService
}

var tracer = otel.Tracer("github.com/nkostiuchenko/payment-gateway/cmd/api")

func NewServer(logger *slog.Logger, paymentService PaymentService) (*Server, error) {
	const op = "NewServer"

	if logger == nil {
		return nil, fmt.Errorf("%s: logger is required", op)
	}

	if paymentService == nil {
		return nil, fmt.Errorf("%s: payment serivce is required", op)
	}

	return &Server{Logger: logger, paymentService: paymentService}, nil
}

func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	const op = "Server.getPayment"

	ctx, span := tracer.Start(r.Context(), "get-payment")
	defer span.End()

	id := r.PathValue("id")

	span.SetAttributes(attribute.String("payment.id", id))

	payment, err := s.paymentService.GetByID(ctx, id)

	if errors.Is(err, service.ErrInvalidId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	if errors.Is(err, service.ErrInvalidPayment) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	if errors.Is(err, repository.ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	json.NewEncoder(w).Encode(payment)
}

func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	const op = "Server.createPayment"

	ctx, span := tracer.Start(r.Context(), "create-payment")
	defer span.End()

	type Request struct {
		Amount   *int             `json:"amount"`
		Currency *string          `json:"currency"`
		Provider *string          `json:"provider"`
		Metadata *json.RawMessage `json:"metadata"`
	}

	var request Request
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	if request.Amount == nil || request.Provider == nil || request.Currency == nil {
		w.WriteHeader(http.StatusBadRequest)
		err := fmt.Errorf("amount, provider and currency must be specified")
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey != "" {
		if _, err := uuid.Parse(idempotencyKey); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			err := fmt.Errorf("Idempotency-Key must be UUID")
			json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
			return
		}
	} else {
		idempotencyKey = uuid.NewString()
	}

	payment, err := s.paymentService.Create(ctx, service.CreatePaymentInput{
		Amount:         *request.Amount,
		Provider:       *request.Provider,
		Currency:       *request.Currency,
		Metadata:       request.Metadata,
		IdempotencyKey: idempotencyKey,
	})

	if errors.Is(err, service.ErrInvalidPayment) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(dto.ErrorResponse{Error: err.Error()})
		return
	}

	type Response struct {
		PaymentID string `json:"payment_id"`
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(Response{PaymentID: payment.ID})
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	const op = "Server.createPayment"

	ctx, span := tracer.Start(r.Context(), op)
	defer span.End()

	id := r.PathValue("id")

	span.SetAttributes(attribute.String("payment.id", id))

	s.paymentService.Webhook(ctx, id)
}

func run() error {
	logger := slog.New(logging.NewOtelID(logging.NewRequestIDHandler(slog.NewJSONHandler(os.Stdout, nil))))

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed load config: %w", err)
	}

	shutdownTracing, err := telemetry.InitTracing(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("otel setup failed: %w", err)
	}

	db, err := db.NewPostgres(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db connection failed: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping failed: %w", err)
	}

	broker, err := broker.NewKafkaBroker(cfg.KafkaURL)
	if err != nil {
		return fmt.Errorf("broker creation failed: %w", err)
	}

	paymentRepository := repository.NewPaymentRepository(db)
	paymentService := service.NewPaymentService(paymentRepository, broker)

	server, err := NewServer(logger, &paymentService)
	if err != nil {
		return fmt.Errorf("server creation failed: %w", err)
	}

	reg := prometheus.NewRegistry()
	goCollector := collectors.NewGoCollector()
	processCollector := collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})
	reg.MustRegister(goCollector, processCollector)
	appMetrics := metrics.NewMetrics(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", server.createPayment)
	mux.HandleFunc("GET /payments/{id}", server.getPayment)
	mux.HandleFunc("POST /webhook/{id}", server.handleWebhook)

	handler := middleware.ContentJSON(mux)
	handler = middleware.RequestLogger(logger)(handler)
	handler = middleware.RequestID(handler)
	handler = otelhttp.NewHandler(handler, "http.server")
	handler = promhttp.InstrumentHandlerCounter(appMetrics.HTTPRequestsTotal, handler)
	handler = promhttp.InstrumentHandlerDuration(appMetrics.HTTPRequestDuration, handler)

	rootMux := http.NewServeMux()
	rootMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	rootMux.Handle("/", handler)

	httpServer := http.Server{
		Addr:    ":8080",
		Handler: rootMux,
	}

	serverErr := make(chan error, 1)

	go func() {
		logger.Info("starting HTTP server", "address", httpServer.Addr)
		serverErr <- httpServer.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	received := <-sig
	logger.Info("shutdown signal received", "signal", received.String())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger.Info("shutting down http server")
	if err := httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server closed with error: %w", err)
	}

	logger.Info("closing db connection")
	if err := db.Close(); err != nil {
		return fmt.Errorf("db close failed: %w", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
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
