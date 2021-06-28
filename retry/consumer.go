package retry

import (
	"fmt"
	"log"
	"time"

	"github.com/peaceman/kafka-rnd-go/kafka"
)

type Consumer struct {
	Config         Config
	Consumer       kafka.Consumer
	MessageHandler MessageHandler
	run            bool
}

func (c *Consumer) Start() (<-chan interface{}, error) {
	c.run = true

	topics := c.generateTopicNames()

	if err := c.Consumer.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}

	doneChan := make(chan interface{})

	go func() {
		for c.run {
			msg, err := c.Consumer.ReadMessage(time.Millisecond * 100)
			if kafka.IsReadTimeout(msg, err) {
				continue
			}

			if err != nil {
				log.Printf("Consumer error: %v (%v)", err, msg)
				continue
			}

			if err := c.MessageHandler.Handle(msg); err != nil {
				log.Printf("Failed to handle message: %v (%v)", msg, err)
				c.Consumer.Seek(msg.TopicPartition, 0)
			} else {
				c.Consumer.CommitMessage(msg)
			}
		}

		close(doneChan)
	}()

	return doneChan, nil
}

func (c *Consumer) Stop() {
	c.run = false
}

func (c *Consumer) generateTopicNames() (topics []string) {
	for _, t := range c.Config.PrimaryTopics {
		topics = append(topics, t, fmt.Sprintf("%s-retry-%s", t, c.Config.ConsumerGroupId))
	}

	return
}
