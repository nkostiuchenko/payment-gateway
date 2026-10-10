package service

import "errors"

var (
	ErrInvalidPayment = errors.New("invalid payment")
	ErrInvalidId      = errors.New("invalid id")
)
