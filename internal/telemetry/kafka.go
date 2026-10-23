package telemetry

import (
	"github.com/IBM/sarama"
)

type ProducerCarrier struct {
	Message *sarama.ProducerMessage
}

func (c ProducerCarrier) Get(key string) string {
	for _, h := range c.Message.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c ProducerCarrier) Set(key, value string) {
	for i := range c.Message.Headers {
		if string(c.Message.Headers[i].Key) == key {
			c.Message.Headers[i].Value = []byte(value)
			return
		}
	}
	c.Message.Headers = append(c.Message.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (c ProducerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Message.Headers))
	for _, h := range c.Message.Headers {
		keys = append(keys, string(h.Key))
	}
	return keys
}

type ConsumerCarrier struct {
	Message *sarama.ConsumerMessage
}

func (c ConsumerCarrier) Get(key string) string {
	for _, h := range c.Message.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c ConsumerCarrier) Set(key, value string) {
	for _, h := range c.Message.Headers {
		if h != nil && string(h.Key) == key {
			h.Value = []byte(value)
			return
		}
	}
	c.Message.Headers = append(c.Message.Headers, &sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (c ConsumerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.Message.Headers))
	for _, h := range c.Message.Headers {
		if h != nil {
			keys = append(keys, string(h.Key))
		}
	}
	return keys
}
