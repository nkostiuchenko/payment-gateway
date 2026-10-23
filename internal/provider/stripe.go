package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/nkostiuchenko/payment-gateway/internal/config"
)

var ErrPaymentFailed = errors.New("payment failed")

type StripeProvider struct {
	cfg config.Config
}

func (s *StripeProvider) Pay(ctx context.Context, amount int, currency string, webhook string) error {
	const op = "StripeProvider.Pay"

	type Body struct {
		Amount   int    `json:"amount"`
		Currency string `json:"currency"`
		Webhook  string `json:"webhook"`
	}

	body := Body{Amount: amount, Currency: currency, Webhook: webhook}
	js, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%s: failed to marshal body: %w", op, err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.StripeURL+"/pay", bytes.NewReader(js))
	if err != nil {
		return fmt.Errorf("%s: failed to create request: %w", op, err)
	}

	client := &http.Client{}

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("%s: failed to send request: %w", op, err)
	}

	defer response.Body.Close()

	return nil
}

func NewStripeProvider(cfg config.Config) *StripeProvider {
	return &StripeProvider{cfg: cfg}
}
