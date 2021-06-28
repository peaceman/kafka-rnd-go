package delayconsumer

import (
	"errors"
	"sync"
	"testing"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/peaceman/kafka-rnd-go/mock"
)

var dcc = Config{
	HeaderNames: HeaderNameConfig{
		ResumeTime:  "resume-time",
		TargetTopic: "target-topic",
	},
	Topics:                []string{"foo", "bar"},
	DeliveryReportTimeout: time.Second,
}

func TestSubscribesToTopics(t *testing.T) {
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.SubscribeTopicsFn = func(s []string, rc ck.RebalanceCb) error {
		if !cmp.Equal(dcc.Topics, s) {
			t.Fatalf(
				"SubscribeTopics was called with unexpected topics %v, expected %v",
				s,
				dcc.Topics,
			)
		}

		return nil
	}

	delayConsumer := &DelayConsumer{
		Consumer: kafkaConsumer,
		Config:   dcc,
	}

	if _, err := delayConsumer.Start(); err != nil {
		t.Fatalf("Failed to start DelayConsumer: %v", err)
	}

	if !kafkaConsumer.SubscribeTopicsInvoked {
		t.Fatal("SubscribeTopics on the kafka consumer was not invoked")
	}
}

func TestStartFailsIfSubscribeTopicsFails(t *testing.T) {
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.SubscribeTopicsFn = func(s []string, rc ck.RebalanceCb) error {
		return errors.New("forced subscription error")
	}

	delayConsumer := &DelayConsumer{
		Consumer: kafkaConsumer,
	}

	if _, err := delayConsumer.Start(); err == nil {
		t.Fatal("Expected the DelayConsumer to fail during start, but it didn't")
	}

	if !kafkaConsumer.SubscribeTopicsInvoked {
		t.Fatal("SubscribeTopics on the kafka consumer was not invoked")
	}
}

func TestDelayConfigParsingInvalidResumeTime(t *testing.T) {
	msg := &ck.Message{}
	messageQueue := []*ck.Message{msg}
	var wg *sync.WaitGroup

	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue(messageQueue)
	kafkaConsumer.CommitMessageFn = func(m *ck.Message) ([]ck.TopicPartition, error) {
		if !cmp.Equal(msg, m) {
			t.Fatalf("Received unexpected msg in CommitMessage: %v expected %v", m, msg)
		}

		return nil, nil
	}
	delayConsumer := &DelayConsumer{
		Consumer: kafkaConsumer,
		Config:   dcc,
	}

	doneChan, _ := delayConsumer.Start()
	wg.Wait()
	delayConsumer.Stop()

	select {
	case <-doneChan:
	case <-time.After(time.Millisecond * 10):
		t.Fatal("Test didn't finish in time")
	}

	if !kafkaConsumer.CommitMessageInvoked {
		t.Fatal("Message with invalid delay config was not committed")
	}
}

func TestDelayConfigMissingTargetTopic(t *testing.T) {
	msg := &ck.Message{
		Headers: []ck.Header{
			{Key: dcc.HeaderNames.ResumeTime, Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}
	var wg *sync.WaitGroup

	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue([]*ck.Message{msg})
	kafkaConsumer.CommitMessageFn = func(m *ck.Message) ([]ck.TopicPartition, error) {
		if !cmp.Equal(msg, m) {
			t.Fatalf("Received unexpected msg in CommitMessage: %v expected %v", m, msg)
		}

		return nil, nil
	}

	delayConsumer := &DelayConsumer{Consumer: kafkaConsumer, Config: dcc}

	doneChan, _ := delayConsumer.Start()
	wg.Wait()
	delayConsumer.Stop()

	select {
	case <-doneChan:
	case <-time.After(time.Millisecond * 10):
		t.Fatal("Test didn't finish in time")
	}

	if !kafkaConsumer.CommitMessageInvoked {
		t.Fatal("Message with missing target topic was not committed")
	}
}

func TestResumeTimeIsReached(t *testing.T) {
	targetTopic := "dis is target topic"
	msg := &ck.Message{
		Headers: []ck.Header{
			{Key: dcc.HeaderNames.ResumeTime, Value: []byte(time.Now().Add(-time.Hour * 2).Format(time.RFC3339))},
			{Key: dcc.HeaderNames.TargetTopic, Value: []byte(targetTopic)},
		},
	}

	var wg *sync.WaitGroup
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue([]*ck.Message{msg})
	kafkaConsumer.CommitMessageFn = func(m *ck.Message) ([]ck.TopicPartition, error) {
		if !cmp.Equal(msg, m) {
			t.Fatalf("Received unexpected msg in CommitMessage: %v expected %v", m, msg)
		}

		return nil, nil
	}

	kafkaProducer := &mock.KafkaProducer{}
	kafkaProducer.ProduceFn = func(m *ck.Message, c chan ck.Event) error {
		if !cmp.Equal(*m.TopicPartition.Topic, targetTopic) {
			t.Fatalf("Expected messaged to be published to %s but got %v", targetTopic, m.TopicPartition.Topic)
		}

		go func() {
			c <- &ck.Message{
				TopicPartition: ck.TopicPartition{},
			}
		}()

		return nil
	}

	delayConsumer := &DelayConsumer{Consumer: kafkaConsumer, Producer: kafkaProducer, Config: dcc}
	doneChan, _ := delayConsumer.Start()
	wg.Wait()
	delayConsumer.Stop()

	select {
	case <-doneChan:
	case <-time.After(time.Millisecond * 10):
		t.Fatal("Test didn't finish in time")
	}

	if !kafkaConsumer.CommitMessageInvoked {
		t.Fatal("Message that reached resume time was not committed")
	}

	if !kafkaProducer.ProduceInvoked {
		t.Fatal("Message that reached resume time was not republished")
	}
}

func TestResumeTimeIsReachedButDeliveryReportTimedOut(t *testing.T) {
	dcc := dcc
	dcc.DeliveryReportTimeout = 0

	targetTopic := "dis is target topic"
	msg := &ck.Message{
		TopicPartition: ck.TopicPartition{
			Topic: nil,
		},
		Headers: []ck.Header{
			{Key: dcc.HeaderNames.ResumeTime, Value: []byte(time.Now().Add(-time.Hour * 2).Format(time.RFC3339))},
			{Key: dcc.HeaderNames.TargetTopic, Value: []byte(targetTopic)},
		},
		Value: []byte("foobar"),
	}

	var wg *sync.WaitGroup
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue([]*ck.Message{msg})

	kafkaProducer := &mock.KafkaProducer{}
	kafkaProducer.ProduceFn = func(m *ck.Message, c chan ck.Event) error {
		if !cmp.Equal(m.Value, msg.Value) {
			t.Fatalf("Received unexpected msg in Produce: %v expected %v", m, msg)
		}

		return nil
	}

	delayConsumer := &DelayConsumer{Consumer: kafkaConsumer, Producer: kafkaProducer, Config: dcc}
	doneChan, _ := delayConsumer.Start()
	wg.Wait()
	delayConsumer.Stop()

	select {
	case <-doneChan:
	case <-time.After(time.Millisecond * 10):
		t.Fatal("Test didn't finish in time")
	}

	if kafkaConsumer.CommitMessageInvoked {
		t.Fatal("Message that could not be republished was committed")
	}

	if !kafkaProducer.ProduceInvoked {
		t.Fatal("Message that reached resume time was not republished")
	}
}

func TestResumeTimeIsNotReached(t *testing.T) {
	targetTopic := "dis is target topic"
	topic := "omega"
	msg := &ck.Message{
		TopicPartition: ck.TopicPartition{
			Topic: &topic,
		},
		Headers: []ck.Header{
			{Key: dcc.HeaderNames.ResumeTime, Value: []byte(time.Now().Add(time.Second).Format(time.RFC3339))},
			{Key: dcc.HeaderNames.TargetTopic, Value: []byte(targetTopic)},
		},
		Value: []byte("foobar"),
	}

	var wg *sync.WaitGroup
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue([]*ck.Message{msg})
	kafkaConsumer.PauseFn = func(tp []ck.TopicPartition) error {
		if !cmp.Equal(tp, []ck.TopicPartition{msg.TopicPartition}) {
			t.Fatalf("Pause received unexpected TopicPartition: %v expected %v", tp, []ck.TopicPartition{msg.TopicPartition})
		}

		return nil
	}
	kafkaConsumer.ResumeFn = func(tp []ck.TopicPartition) error {
		if !cmp.Equal(tp, []ck.TopicPartition{msg.TopicPartition}) {
			t.Fatalf("Resume received unexpected TopicPartition: %v expected %v", tp, []ck.TopicPartition{msg.TopicPartition})
		}

		return nil
	}
	kafkaProducer := &mock.KafkaProducer{}

	delayConsumer := &DelayConsumer{Consumer: kafkaConsumer, Producer: kafkaProducer, Config: dcc}
	doneChan, _ := delayConsumer.Start()
	wg.Wait()

	time.Sleep(time.Second)
	delayConsumer.Stop()

	select {
	case <-doneChan:
	case <-time.After(time.Millisecond * 10):
		t.Fatal("Test didn't finish in time")
	}

	if kafkaConsumer.CommitMessageInvoked {
		t.Fatal("Message that could not be republished was committed")
	}

	if !kafkaConsumer.ResumeInvoked {
		t.Fatalf("Topic consumption was not resumed")
	}

	if kafkaProducer.ProduceInvoked {
		t.Fatal("Message that didn't reach resume time was republished")
	}
}
