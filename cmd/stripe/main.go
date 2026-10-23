package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nkostiuchenko/payment-gateway/internal/logging"
	"github.com/nkostiuchenko/payment-gateway/internal/middleware"
)

const (
	avgDelay   = 5000
	rejectProb = 0.1
	timeoutSec = 10
)

type Payment struct {
	Currency string
	Amount   int64
	Webhook  string
	Intent   string
}

func ProcessPayment(payment Payment) {
	delay := mrand.Intn(avgDelay)
	isError := mrand.Float64() <= rejectProb

	<-time.After(time.Duration(delay) * time.Millisecond)

	type Body struct {
		Status string `json:"status"`
		Intent string `json:"intent"`
	}

	var status string
	if isError {
		status = "error"
	} else {
		status = "success"
	}

	body := Body{Status: status, Intent: payment.Intent}

	js, err := json.Marshal(body)
	if err != nil {
		slog.Error("body serialization failed", "error", err)
		return
	}

	request, err := http.NewRequest(http.MethodPost, payment.Webhook, bytes.NewReader(js))
	if err != nil {
		slog.Error("request creation failed", "error", err)
		return
	}

	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: timeoutSec * time.Second}

	response, err := client.Do(request)
	if err != nil {
		slog.Error("request send failed", "error", err)
		return
	}

	defer response.Body.Close()
}

func NewIntent() string {
	var intent [16]byte
	crand.Read(intent[:])
	return hex.EncodeToString(intent[:])
}

func handlePay(w http.ResponseWriter, r *http.Request) {
	type Request struct {
		Amount   *int64  `json:"amount"`
		Currency *string `json:"currency"`
		Webhook  *string `json:"webhook"`
	}

	var request Request
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{ "error": "malformed input" }`))
		return
	}

	if request.Amount == nil || request.Currency == nil || request.Webhook == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{ "error": "fields amount, currency and webhook required" }`))
		return
	}

	type Response struct {
		Intent string `json:"intent"`
	}

	intent := NewIntent()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(Response{Intent: intent})

	payment := Payment{Amount: *request.Amount, Currency: *request.Currency, Webhook: *request.Webhook, Intent: intent}
	go ProcessPayment(payment)
}

func run() error {
	slog.SetDefault(slog.New(logging.NewRequestIDHandler(slog.NewJSONHandler(os.Stdout, nil))))

	mux := http.NewServeMux()
	mux.Handle("POST /pay", middleware.RequestID(middleware.RequestLogger(slog.Default())(http.HandlerFunc(handlePay))))

	httpServer := http.Server{Addr: ":8080", Handler: mux}

	serverErr := make(chan error, 1)

	go func() {
		slog.Info("starting HTTP server", "address", httpServer.Addr)
		serverErr <- httpServer.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	received := <-sig
	slog.Info("shutdown signal received", "signal", received.String())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	slog.Info("shutting down http server")
	if err := httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server closed with error: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}
