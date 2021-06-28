package delayconsumer

import (
	"errors"
	"log"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/peaceman/kafka-rnd-go/kafka"
)

type Config struct {
	HeaderNames           HeaderNameConfig
	Topics                []string
	DeliveryReportTimeout time.Duration
}

type HeaderNameConfig struct {
	ResumeTime  string
	TargetTopic string
}

func (c *HeaderNameConfig) names() []string {
	return []string{
		c.ResumeTime,
		c.TargetTopic,
	}
}

type messageDelayConfig struct {
	targetTopic string
	resumeTime  time.Time
}

type DelayConsumer struct {
	Config   Config
	Consumer kafka.Consumer
	Producer kafka.Producer
	run      bool
}

func (c *DelayConsumer) Start() (<-chan interface{}, error) {
	c.run = true

	doneChan := make(chan interface{})

	if err := c.Consumer.SubscribeTopics(c.Config.Topics, nil); err != nil {
		return nil, err
	}

	go func() {
		for c.run {
			msg, err := c.Consumer.ReadMessage(time.Millisecond * 100)
			if isReadTimeout(msg, err) {
				continue
			}

			if err != nil {
				log.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}

			delayConfig, err := c.parseDelayConfigFromMessage(msg)
			if err != nil {
				log.Println("Failed to parse delay config from message; skipping message", err)
				c.Consumer.CommitMessage(msg)
				continue
			}

			if time.Now().Before(delayConfig.resumeTime) {
				// resume time is not reached
				// pause the topic consumption and spawn a goroutine that will resume
				// topic consumption when the resume time is reached

				c.Consumer.Pause([]ck.TopicPartition{msg.TopicPartition})
				go c.resumeTopicConsumption(msg.TopicPartition, delayConfig.resumeTime)
				c.Consumer.Seek(msg.TopicPartition, 0)
			} else {
				log.Println(
					"Message reached resume time",
					msg,
				)

				if err := c.republishMessage(delayConfig.targetTopic, msg); err != nil {
					c.Consumer.Seek(msg.TopicPartition, 0)
				} else {
					c.Consumer.CommitMessage(msg)
				}
			}
		}

		close(doneChan)
	}()

	return doneChan, nil
}

func (c *DelayConsumer) Stop() {
	c.run = false
}

func (c *DelayConsumer) resumeTopicConsumption(topicPartition ck.TopicPartition, resumeTime time.Time) {
	sleepTime := time.Until(resumeTime)

	log.Printf("Sleeping %v before resuming topic consumption %s", sleepTime, *topicPartition.Topic)
	time.Sleep(sleepTime)

	log.Printf("Resuming topic consumption %s", *topicPartition.Topic)
	c.Consumer.Resume([]ck.TopicPartition{topicPartition})
}

func (c *DelayConsumer) republishMessage(targetTopic string, msg *ck.Message) error {
	pmsg := *msg
	kafka.RemoveHeaders(c.Config.HeaderNames.names(), &pmsg)
	pmsg.TopicPartition.Topic = &targetTopic

	deliveryChan := make(chan ck.Event)
	err := c.Producer.Produce(&pmsg, deliveryChan)
	if err != nil {
		return err
	}

	log.Printf("Wait for the kafka delivery report")
	select {
	case deliveryReport := <-deliveryChan:
		deliveryMessage := deliveryReport.(*ck.Message)
		return deliveryMessage.TopicPartition.Error
	case <-time.After(c.Config.DeliveryReportTimeout):
		return errors.New("waiting for the delivery report timed out")
	}
}

func isReadTimeout(msg *ck.Message, err error) bool {
	if err == nil {
		return false
	}

	kafkaError, ok := err.(ck.Error)
	if !ok {
		return false
	}

	return kafkaError.Code() == ck.ErrTimedOut
}

func (c *DelayConsumer) parseDelayConfigFromMessage(msg *ck.Message) (*messageDelayConfig, error) {
	resumeTimeHeaderValue := string(kafka.SearchHeaderValue(msg.Headers, c.Config.HeaderNames.ResumeTime))
	resumeTime, err := time.Parse(time.RFC3339, resumeTimeHeaderValue)
	if err != nil {
		return nil, err
	}

	targetTopic := string(kafka.SearchHeaderValue(msg.Headers, c.Config.HeaderNames.TargetTopic))
	if targetTopic == "" {
		return nil, errors.New("missing target topic in message headers")
	}

	return &messageDelayConfig{
		resumeTime:  resumeTime,
		targetTopic: targetTopic,
	}, nil
}
