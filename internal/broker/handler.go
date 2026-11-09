package broker

import (
	"context"

	"github.com/IBM/sarama"
)

type MessageProcessor func(ctx context.Context, msg *sarama.ConsumerMessage) error

type Handler struct {
	processor MessageProcessor
}

func (h Handler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h Handler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil

		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if err := h.processor(session.Context(), message); err != nil {
				return err
			}

			session.MarkMessage(message, "")
		}
	}
}

func NewHandler(processor MessageProcessor) *Handler {
	return &Handler{processor: processor}
}
