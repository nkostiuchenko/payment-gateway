package provider

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var ErrPaymentFailed = errors.New("payment failed")

type StripeProvider struct {
}

func (s *StripeProvider) Pay(ctx context.Context, amount int, currency string) error {
	const op = "StripeProvider.Pay"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	delay := rand.Intn(5000)
	isError := rand.Float64() <= 0.1

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(delay) * time.Millisecond):
	}

	if isError {
		return fmt.Errorf("%s: %w", op, ErrPaymentFailed)
	}

	return nil
}

func NewStripeProvider() *StripeProvider {
	return &StripeProvider{}
}
