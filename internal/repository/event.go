package repository

import (
	"context"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type EventRepository struct {
	collection *mongo.Collection
}

func (r *EventRepository) Save(ctx context.Context, event *model.Event) error {
	const op = "EventRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	_, err := r.collection.UpdateOne(
		ctx,
		bson.M{"id": event.ID},
		bson.M{"$set": event},
		options.UpdateOne().SetUpsert(true),
	)

	if err != nil {
		return fmt.Errorf("%s: save event: %w", op, err)
	}

	return nil
}

func NewEventRepository(collection *mongo.Collection) *EventRepository {
	return &EventRepository{collection: collection}
}
