package repository

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/codes"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
)

type PaymentRepository struct {
	collection *mongo.Collection
}

func (r *PaymentRepository) Save(ctx context.Context, payment *model.Payment) error {
	const op = "PaymentRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	_, err := r.collection.UpdateOne(
		ctx,
		bson.M{"id": payment.ID},
		bson.M{"$set": payment},
		options.UpdateOne().SetUpsert(true),
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("%s: save payment: %w", op, err)
	}

	return nil
}

func (r *PaymentRepository) GetByID(ctx context.Context, id string) (*model.Payment, error) {
	const op = "PaymentRepository.GetByID"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	result := r.collection.FindOne(ctx, bson.M{"id": id})

	if err := result.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%s: payment not found: %w", op, ErrNotFound)
		}
		return nil, fmt.Errorf("%s: payment query: %w", op, err)
	}

	var payment model.Payment
	if err := result.Decode(&payment); err != nil {
		return nil, fmt.Errorf("%s: payment decode failed: %w", op, err)
	}

	return &payment, nil
}

func (r *PaymentRepository) GetByIdempotencyKey(ctx context.Context, key string) (*model.Payment, error) {
	const op = "PaymentRepository.GetByIdempotencyKey"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	result := r.collection.FindOne(ctx, bson.M{"idempotencykey": key})

	if err := result.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%s: payment not found: %w", op, ErrNotFound)
		}
		return nil, fmt.Errorf("%s: payment query: %w", op, err)
	}

	var payment model.Payment
	if err := result.Decode(&payment); err != nil {
		return nil, fmt.Errorf("%s: payment decode failed: %w", op, err)
	}

	return &payment, nil
}

func NewPaymentRepository(collection *mongo.Collection) *PaymentRepository {
	return &PaymentRepository{collection: collection}
}
