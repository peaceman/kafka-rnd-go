package kafka

import (
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer interface {
	ReadMessage(time.Duration) (*ck.Message, error)
	Subscribe(string, ck.RebalanceCb) error
	SubscribeTopics(topics []string, rebalanceCb ck.RebalanceCb) (err error)
	Seek(partition ck.TopicPartition, timeoutMs int) error
	CommitMessage(*ck.Message) ([]ck.TopicPartition, error)
	Pause([]ck.TopicPartition) (err error)
	Resume([]ck.TopicPartition) (err error)
	Close() error
}
