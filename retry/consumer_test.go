package retry

import (
	"errors"
	"sort"
	"sync"
	"testing"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/peaceman/kafka-rnd-go/mock"
)

var cc = Config{
	PrimaryTopics:   []string{"pri-a", "pri-b"},
	DelayTopic:      "delay",
	ConsumerGroupId: "cgrpid",
	HeaderNames: HeaderNameConfig{
		ResumeTime:  "resume-time",
		TargetTopic: "target-topic",
	},
}
var stringSortTransformer = cmp.Transformer("Sort", func(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)

	return out
})

func TestSubscribesToTopics(t *testing.T) {
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.SubscribeTopicsFn = func(s []string, rc ck.RebalanceCb) error {
		et := []string{
			"pri-a",
			"pri-b",
			"pri-a-retry-cgrpid",
			"pri-b-retry-cgrpid",
		}

		if !cmp.Equal(s, et, stringSortTransformer) {
			t.Fatalf(
				"SubscribeTopics was called with unexpected topics %v, expected %v",
				s,
				et,
			)
		}

		return nil
	}

	retryConsumer := &Consumer{
		Config:   cc,
		Consumer: kafkaConsumer,
	}

	if _, err := retryConsumer.Start(); err != nil {
		t.Fatalf("Failed to start RetryConsumer: %v", err)
	}

	if !kafkaConsumer.SubscribeTopicsInvoked {
		t.Fatalf("SubscribeTopics was not called")
	}
}

func TestStartFailsIfSubscribeTopicsFails(t *testing.T) {
	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.SubscribeTopicsFn = func(s []string, rc ck.RebalanceCb) error {
		return errors.New("forced subscription error")
	}

	retryConsumer := &Consumer{
		Config:   cc,
		Consumer: kafkaConsumer,
	}

	if _, err := retryConsumer.Start(); err == nil {
		t.Fatalf("Expected the RetryConsumer to fail during start, but it didn't")
	}
}

func TestSendsMessagesToTheMessageHandler(t *testing.T) {
	var wg *sync.WaitGroup
	msg := &ck.Message{}

	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue([]*ck.Message{
		msg,
	})
	kafkaConsumer.CommitMessageFn = func(m *ck.Message) ([]ck.TopicPartition, error) {
		if !cmp.Equal(m, msg) {
			t.Fatalf("CommitMessage received unexpected message: %v != %v", msg, m)
		}

		return nil, nil
	}

	var receivedMessage *ck.Message

	retryConsumer := &Consumer{
		Config:   cc,
		Consumer: kafkaConsumer,
		MessageHandler: MessageHandlerFunc(func(m *ck.Message) error {
			receivedMessage = m
			return nil
		}),
	}

	if _, err := retryConsumer.Start(); err != nil {
		t.Fatalf("Failed to start the RetryConsumer: %v", err)
	}

	wg.Wait()

	if !cmp.Equal(msg, receivedMessage) {
		t.Fatalf("MessageHandler expected %v but received %v", msg, receivedMessage)
	}

	if !kafkaConsumer.CommitMessageInvoked {
		t.Fatal("CommitMessage on the kafka consumer was not invoked")
	}
}

func TestErrorFromTheMessageHandlerResultsInNotCommittingTheMessage(t *testing.T) {
	var wg *sync.WaitGroup
	msg := &ck.Message{
		TopicPartition: ck.TopicPartition{
			Topic: new(string),
		},
	}

	kafkaConsumer := mock.NewKafkaConsumer()
	kafkaConsumer.ReadMessageFn, wg = mock.CreateReadMessageFnFromMessageQueue([]*ck.Message{
		msg,
	})
	kafkaConsumer.SeekFn = func(tp ck.TopicPartition, timeoutMs int) error {
		if !cmp.Equal(msg.TopicPartition, tp) {
			t.Fatalf("Received unexpected topic partition %v != %v", msg.TopicPartition, tp)
		}

		return nil
	}

	retryConsumer := &Consumer{
		Config:   cc,
		Consumer: kafkaConsumer,
		MessageHandler: MessageHandlerFunc(func(m *ck.Message) error {
			return errors.New("forced error")
		}),
	}

	doneChan, err := retryConsumer.Start()
	if err != nil {
		t.Fatalf("Failed to start the RetryConsumer: %v", err)
	}

	wg.Wait()
	retryConsumer.Stop()

	<-doneChan

	if kafkaConsumer.CommitMessageInvoked {
		t.Fatal("CommitMessage on the kafka consumer was invoked but it shouldn't have")
	}

	if !kafkaConsumer.SeekInvoked {
		t.Fatalf("Seek on the kafka consumer was not invoked")
	}
}
