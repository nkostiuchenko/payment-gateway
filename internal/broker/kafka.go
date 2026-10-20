package broker

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

type Kafka struct {
	producer sarama.SyncProducer
}

func (k *Kafka) Publish(ctx context.Context, topic string, content []byte) error {
	const op = "Kafka.Publish"

	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(content),
	})

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func NewKafkaBroker(address string) (*Kafka, error) {
	const op = "NewKafkaBroker"

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{address}, config)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Kafka{producer: producer}, nil
}
