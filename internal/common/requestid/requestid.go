package requestid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type ctxRequestIDKey struct{}

func New() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func FromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxRequestIDKey{}).(string)
	return id, ok
}

func NewContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxRequestIDKey{}, id)
}
