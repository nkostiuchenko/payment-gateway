package dto

type PaymentCreate struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}
