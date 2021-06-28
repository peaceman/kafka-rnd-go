package mock

import (
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateReadMessageFnFromMessageQueue(
	mq []*ck.Message,
) (func(time.Duration) (*ck.Message, error), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(len(mq))

	return func(d time.Duration) (*ck.Message, error) {
		if len(mq) != 0 {
			m := mq[0]
			mq = mq[1:]

			wg.Done()

			return m, nil
		} else {
			return nil, NewReadTimeoutError()
		}
	}, wg
}

type KafkaConsumer struct {
	ReadMessageFn      func(time.Duration) (*ck.Message, error)
	ReadMessageInvoked bool

	SubscribeFn      func(string, ck.RebalanceCb) error
	SubscribeInvoked bool

	SubscribeTopicsFn      func([]string, ck.RebalanceCb) error
	SubscribeTopicsInvoked bool

	SeekFn      func(ck.TopicPartition, int) error
	SeekInvoked bool

	CommitMessageFn      func(*ck.Message) ([]ck.TopicPartition, error)
	CommitMessageInvoked bool

	PauseFn      func([]ck.TopicPartition) (err error)
	PauseInvoked bool

	ResumeFn      func([]ck.TopicPartition) (err error)
	ResumeInvoked bool

	CloseFn      func() error
	CloseInvoked bool
}

func (c *KafkaConsumer) ReadMessage(timeout time.Duration) (*ck.Message, error) {
	c.ReadMessageInvoked = true

	return c.ReadMessageFn(timeout)
}

func (c *KafkaConsumer) Subscribe(topic string, rebalanceCb ck.RebalanceCb) error {
	c.SubscribeInvoked = true

	return c.SubscribeFn(topic, rebalanceCb)
}

func (c *KafkaConsumer) SubscribeTopics(topics []string, rebalanceCb ck.RebalanceCb) error {
	c.SubscribeTopicsInvoked = true

	return c.SubscribeTopicsFn(topics, rebalanceCb)
}

func (c *KafkaConsumer) Seek(topicPartition ck.TopicPartition, timeoutMs int) error {
	c.SeekInvoked = true

	if c.SeekFn != nil {
		return c.SeekFn(topicPartition, timeoutMs)
	} else {
		return nil
	}
}

func (c *KafkaConsumer) CommitMessage(msg *ck.Message) ([]ck.TopicPartition, error) {
	c.CommitMessageInvoked = true

	if c.CommitMessageFn != nil {
		return c.CommitMessageFn(msg)
	} else {
		return make([]ck.TopicPartition, 0), nil
	}
}

func (c *KafkaConsumer) Pause(topicPartitions []ck.TopicPartition) error {
	c.PauseInvoked = true

	return c.PauseFn(topicPartitions)
}

func (c *KafkaConsumer) Resume(topicPartitions []ck.TopicPartition) error {
	c.ResumeInvoked = true

	return c.ResumeFn(topicPartitions)
}

func (c *KafkaConsumer) Close() error {
	c.CloseInvoked = true

	return c.CloseFn()
}

func NewKafkaConsumer() *KafkaConsumer {
	return &KafkaConsumer{
		ReadMessageFn: func(d time.Duration) (*ck.Message, error) {
			return nil, NewReadTimeoutError()
		},
		SubscribeTopicsFn: func(s []string, rc ck.RebalanceCb) error {
			return nil
		},
		CloseFn: func() error {
			return nil
		},
	}
}

func NewReadTimeoutError() error {
	return ck.NewError(ck.ErrTimedOut, "read message timeout", false)
}
