package kafka

import ck "github.com/confluentinc/confluent-kafka-go/kafka"

type Producer interface {
	Close()
	Produce(*ck.Message, chan ck.Event) error
}
