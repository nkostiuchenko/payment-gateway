package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/nkostiuchenko/payment-gateway/internal/config"
	"github.com/nkostiuchenko/payment-gateway/internal/metrics"
	"github.com/nkostiuchenko/payment-gateway/internal/telemetry"

	"github.com/XSAM/otelsql"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var (
	ErrNotFound = errors.New("not found")
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type Payment struct {
	ID        string    `json:"id"`
	Amount    float64   `json:"amount"`
	Provider  string    `json:"provider"`
	Status    string    `json:"status"`
	Metadata  any       `json:"metadata"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Store interface {
	GetByID(id string) (Payment, error)
	Create(Payment) (string, error)
}

type PostgresStore struct {
	db *sql.DB
}

func (p *PostgresStore) GetByID(id string) (Payment, error) {
	const op = "PostgresStore.GetByID"

	const query = `
	SELECT id, amount, provider, status, metadata, created_at, updated_at
	FROM payments
	WHERE id = $1
	`

	row := p.db.QueryRow(query, id)

	var payment Payment
	var metadata []byte

	err := row.Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Provider,
		&payment.Status,
		&metadata,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return Payment{}, fmt.Errorf("%s: %w", op, ErrNotFound)
	}

	if err != nil {
		return Payment{}, fmt.Errorf("%s: %w", op, err)
	}

	if err := json.Unmarshal(metadata, &payment.Metadata); err != nil {
		return Payment{}, fmt.Errorf("%s: %w", op, err)
	}

	return payment, nil
}

func (p *PostgresStore) Create(payment Payment) (string, error) {
	const op = "PostgresStore.Save"

	payment.Status = "NEW"

	tx, err := p.db.BeginTx(context.Background(), nil)
	defer tx.Rollback()

	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	const paymentsQuery = `
	INSERT INTO payments (amount, provider, status, metadata)
	VALUES ($1, $2, $3, $4)
	RETURNING id;
	`

	row := tx.QueryRow(
		paymentsQuery,
		payment.Amount,
		payment.Provider,
		payment.Status,
		payment.Metadata,
	)

	var paymentID string
	if err := row.Scan(&paymentID); err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	const eventsQuery = `
	INSERT INTO payment_events (payment_id, payload)
	VALUES ($1, $2)
	RETURNING id;
	`

	js, err := json.Marshal(payment)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	row = tx.QueryRow(eventsQuery, paymentID, js)

	var eventID string
	if err := row.Scan(&eventID); err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	tx.Commit()

	return paymentID, nil
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

type Server struct {
	Logger *slog.Logger
	Store  Store
}

func NewServer(logger *slog.Logger, store Store) (*Server, error) {
	const op = "NewServer"

	if logger == nil {
		return nil, fmt.Errorf("%s: logger is required", op)
	}

	if store == nil {
		return nil, fmt.Errorf("%s: store is required", op)
	}

	return &Server{Logger: logger, Store: store}, nil
}

func (s *Server) getPaymentHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	payment, err := s.Store.GetByID(id)

	if errors.Is(err, ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	json.NewEncoder(w).Encode(payment)
}

func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	const op = "Server.createPayment"

	type Request struct {
		Amount   *float64 `json:"amount"`
		Provider *string  `json:"provider"`
		Metadata *any     `json:"metadata"`
	}

	var request Request
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	if request.Amount == nil || request.Provider == nil || request.Metadata == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id, err := s.Store.Create(Payment{Amount: *request.Amount, Provider: *request.Provider, Metadata: *request.Metadata})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	payment, err := s.Store.GetByID(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(payment)
}

func contentJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed load config: %w", err)
	}

	shutdownTracing, err := telemetry.InitTracing(context.Background())
	if err != nil {
		return fmt.Errorf("otel setup failed: %w", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := shutdownTracing(ctx); err != nil {
			logger.Error("otel shutdown failed", "error", err)
		}
	}()

	db, err := otelsql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db connection failed: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping failed: %w", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			logger.Error("db close failed", "error", err)
		}
	}()

	db.Exec(`
	CREATE TABLE IF NOT EXISTS payments (
		id 			UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		amount 		NUMERIC(18, 2) NOT NULL,
		provider 	TEXT NOT NULL,
		status 		TEXT NOT NULL,
		metadata 	JSONB,
		created_at 	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at 	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE TABLE IF NOT EXISTS payment_events (
		id 			UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		payment_id 	UUID NOT NULL REFERENCES payments(id),
		payload 	JSONB NOT NULL,
		created_at 	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	`)

	store := NewPostgresStore(db)

	server, err := NewServer(logger, store)
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
	mux.HandleFunc("GET /payments/{id}", server.getPaymentHandler)

	handler := contentJSON(mux)
	handler = otelhttp.NewHandler(handler, "http.server")
	handler = promhttp.InstrumentHandlerCounter(appMetrics.HTTPRequestsTotal, handler)
	handler = promhttp.InstrumentHandlerDuration(appMetrics.HTTPRequestDuration, handler)

	rootMux := http.NewServeMux()
	rootMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	rootMux.Handle("/", handler)

	logger.Info("starting HTTP server", "port", 8080)

	if err := http.ListenAndServe(":8080", rootMux); err != nil {
		return fmt.Errorf("server stopped: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("applicaiton failed", "error", err)
		os.Exit(1)
	}
}
