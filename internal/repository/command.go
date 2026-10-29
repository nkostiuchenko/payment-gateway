package repository

import (
	"context"
	"fmt"

	"github.com/nkostiuchenko/payment-gateway/internal/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CommandRepository struct {
	collection *mongo.Collection
}

func (r *CommandRepository) Save(ctx context.Context, command *model.Command) error {
	const op = "CommandRepository.Save"

	ctx, span := tracer.Start(ctx, op)
	defer span.End()

	_, err := r.collection.UpdateOne(
		ctx,
		bson.M{"id": command.ID},
		bson.M{"$set": command},
		options.UpdateOne().SetUpsert(true),
	)

	if err != nil {
		return fmt.Errorf("%s: save command: %w", op, err)
	}

	return nil
}

func NewCommandRepository(collection *mongo.Collection) *CommandRepository {
	return &CommandRepository{collection: collection}
}
