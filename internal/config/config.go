package config

import (
	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv        string `env:"APP_ENV" envDefault:"development"`
	AppName       string `env:"APP_NAME,required"`
	DatabaseURL   string `env:"DATABASE_URL,required"`
	Debug         bool   `env:"DEBUG" envDefault:"false"`
	KafkaURL      string `env:"KAFKA_URL,required"`
	StripeURL     string `env:"STRIPE_URL,required"`
	WebhookURL    string `env:"WEBHOOK_URL,required"`
	MongoUsername string `env:"MONGO_USERNAME,required"`
	MongoPassword string `env:"MONGO_PASSWORD,required"`
	MongoDB       string `env:"MONGO_DB,required"`
}

func Load() (Config, error) {
	_ = godotenv.Load()

	var cfg Config

	if err := env.Parse(&cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
