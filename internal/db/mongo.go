package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func NewMongo(host string, user string, password string, db string) (*mongo.Database, error) {
	creds := options.Credential{Username: user, Password: password}

	client, err := mongo.Connect(options.Client().SetHosts([]string{host}).SetAuth(creds))

	if err != nil {
		return nil, fmt.Errorf("db connection failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	return client.Database(db), nil
}
