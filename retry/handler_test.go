package retry

import (
	"errors"
	"testing"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/peaceman/kafka-rnd-go/kafka"
	"github.com/peaceman/kafka-rnd-go/mock"
)

var hc = MessageHandlerConfig{
	Config: Config{
		PrimaryTopics:   []string{"pri-a", "pri-b"},
		DelayTopic:      "delay",
		ConsumerGroupId: "cgrpid",
		HeaderNames: HeaderNameConfig{
			ResumeTime:  "resume-time",
			TargetTopic: "target-topic",
			MessageId: "message-id",
		},
	},
	DeliveryReportTimeout: time.Millisecond,
}

func createValidMessage(msgKey string) *ck.Message {
	return &ck.Message{
		Key:   []byte(msgKey),
		Value: []byte("dis is msg value"),
		Headers: []ck.Header{
			{
				Key:   hc.HeaderNames.MessageId,
				Value: []byte(uuid.New().String()),
			},
		},
	}
}

func getMessageId(msg *ck.Message) string {
	return string(kafka.SearchHeaderValue(msg.Headers, hc.Config.HeaderNames.MessageId))
}

func TestHandler_IgnoresMessagesWithoutId(t *testing.T) {
	msgKey := "dis is msg key"
	msg := createValidMessage(msgKey)
	msg.Headers = nil

	handler := &RetryMessageHandler{
		Config: hc,
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatal(err)
	}
}

func TestHandler_RepublishesToDelayIfThereAreAlreadyFailuresInThisTry(t *testing.T) {
	msgKey := "dis is msg key"
	msg := createValidMessage(msgKey)

	kafkaProducer := &mock.KafkaProducer{
		ProduceFn: func(m *ck.Message, c chan ck.Event) error {
			if !cmp.Equal(*m.TopicPartition.Topic, hc.DelayTopic) {
				t.Fatalf(
					"Delay topic was not set correctly: %v != %v",
					hc.DelayTopic,
					*m.TopicPartition.Topic,
				)
			}

			resumeTimeString := string(kafka.SearchHeaderValue(m.Headers, hc.HeaderNames.ResumeTime))
			if resumeTimeString == "" {
				t.Fatal("Resume time header was not set")
			}

			if _, err := time.Parse(time.RFC3339, resumeTimeString); err != nil {
				t.Fatalf("Failed to parse resume time header value: %v", err)
			}

			go func() {
				c <- &ck.Message{}
			}()

			return nil
		},
	}

	failureStorage := &mockFailureStorage{
		HasFailedFn: func(s string, u uint) (bool, error) {
			if !cmp.Equal(msgKey, s) {
				t.Fatalf("Received unexpected message key: %v != %v", msgKey, s)
			}

			if u != 0 {
				t.Fatalf("Received unexpected try counter: 0 != %v", u)
			}

			return true, nil
		},
		MarkFailureFn: func(s string, u uint, id string) error {
			if !cmp.Equal(msgKey, s) {
				t.Fatalf("Received unexpected message key: %v != %v", msgKey, s)
			}

			if u != 0 {
				t.Fatalf("Received unexpected try counter: 0 != %v", u)
			}

			if id != getMessageId(msg) {
				t.Fatalf("Received unexpected message id: %v != %v", getMessageId(msg), id)
			}

			return nil
		},
	}

	handler := &RetryMessageHandler{
		Config:         hc,
		Producer:       kafkaProducer,
		FailureStorage: failureStorage,
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatal(err)
	}

	if !failureStorage.HasFailedInvoked {
		t.Fatal("HasFailed was not invoked")
	}

	if !kafkaProducer.ProduceInvoked {
		t.Fatal("Produce was not invoked")
	}

	if !failureStorage.MarkFailureInvoked {
		t.Fatal("MarkFailure was not invoked")
	}
}

func TestHandler_HandleTryHeaderParsingErrorGracefully(t *testing.T) {
	msgKey := "dis is msg key"
	msg := createValidMessage(msgKey)
	msg.Headers = append(msg.Headers, ck.Header{
		Key: hc.HeaderNames.Try,
		Value: []byte("not a number"),
	})

	failureStorage := &mockFailureStorage{
		HasFailedFn: func(s string, u uint) (bool, error) {
			if u != 0 {
				t.Fatalf("Received unexpected try count: %v", u)
			}

			return false, nil
		},
	}

	handler := &RetryMessageHandler{
		Config:         hc,
		FailureStorage: failureStorage,
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatal(err)
	}

	if !failureStorage.HasFailedInvoked {
		t.Fatal("HasFailed was not invoked")
	}
}

func TestHandler_FailureStorageErrorsAreReturned(t *testing.T) {
	msgKey := "dis is msg key"
	msg := createValidMessage(msgKey)

	failureStorage := &mockFailureStorage{}

	handler := &RetryMessageHandler{
		Config:         hc,
		FailureStorage: failureStorage,
	}

	t.Run("HasFailedError", func(t *testing.T) {
		failureStorage.HasFailedFn = func(s string, u uint) (bool, error) {
			return false, errors.New("forced error")
		}

		err := handler.Handle(msg)
		if err == nil || err.Error() != "forced error" {
			t.Fatal("Failure storage error was not returned")
		}
	})

	t.Run("MarkFailureError", func(t *testing.T) {
		failureStorage.HasFailedFn = func(s string, u uint) (bool, error) {
			return true, nil
		}
		failureStorage.MarkFailureFn = func(s string, u uint, id string) error {
			return errors.New("forced error")
		}

		err := handler.Handle(msg)
		if err == nil || err.Error() != "forced error" {
			t.Fatal("Failure storage error was not returned")
		}
	})
}

func TestHandler_RepublishError(t *testing.T) {
	msgKey := "dis is msg key"
	msg := &ck.Message{
		Key:   []byte(msgKey),
		Value: []byte("dis is msg value"),
		Headers: []ck.Header{
			{
				Key:   hc.HeaderNames.Try,
				Value: []byte("not a number"),
			},
			{
				Key:   hc.HeaderNames.MessageId,
				Value: []byte(uuid.New().String()),
			},
		},
	}
	kafkaProducer := &mock.KafkaProducer{}

	failureStorage := &mockFailureStorage{
		HasFailedFn: func(s string, u uint) (bool, error) {
			return true, nil
		},
	}

	handler := &RetryMessageHandler{
		Config:         hc,
		Producer:       kafkaProducer,
		FailureStorage: failureStorage,
	}

	t.Run("ProduceError", func(t *testing.T) {
		kafkaProducer.ProduceFn = func(m *ck.Message, c chan ck.Event) error {
			return errors.New("forced produce error")
		}

		if err := handler.Handle(msg); err == nil {
			t.Fatal("Missing propagated Produce error")
		}
	})

	t.Run("DeliveryReportTimeoutError", func(t *testing.T) {
		kafkaProducer.ProduceFn = func(m *ck.Message, c chan ck.Event) error {
			return nil
		}

		if err := handler.Handle(msg); err == nil {
			t.Fatal("Missing propagated DeliveryReportTimeout error")
		}
	})
}

func TestHandle_HandleMessage(t *testing.T) {
	msgKey := "dis is msg key"
	msg := createValidMessage(msgKey)

	kafkaProducer := &mock.KafkaProducer{}
	failureStorage := &mockFailureStorage{
		HasFailedFn: func(s string, u uint) (bool, error) {
			return false, nil
		},
		MarkSuccessFn: func(s string, id string) error {
			if s != msgKey {
				t.Fatalf("Received unexpected message key: %s != %s", msgKey, s)
			}

			if id != getMessageId(msg) {
				t.Fatalf("Received unexpected message id: %v != %v", getMessageId(msg), id)
			}

			return nil
		},
	}

	var receivedMessage *ck.Message

	handler := &RetryMessageHandler{
		Config:         hc,
		Producer:       kafkaProducer,
		FailureStorage: failureStorage,
		Next: MessageHandlerFunc(func(m *ck.Message) error {
			receivedMessage = m
			return nil
		}),
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(msg, receivedMessage) {
		t.Fatalf(
			"The wrapped message handler received an unexpected message: %v != %v",
			msg,
			receivedMessage,
		)
	}

	if !failureStorage.MarkSuccessInvoked {
		t.Fatal("MarkSuccess was not called")
	}
}

func TestHandle_HandleMessageError(t *testing.T) {
	msgKey := "dis is msg key"
	msg := createValidMessage(msgKey)

	kafkaProducer := &mock.KafkaProducer{
		ProduceFn: func(m *ck.Message, c chan ck.Event) error {
			if !cmp.Equal(*m.TopicPartition.Topic, hc.DelayTopic) {
				t.Fatalf(
					"Delay topic was not set correctly: %v != %v",
					hc.DelayTopic,
					*m.TopicPartition.Topic,
				)
			}

			resumeTimeString := string(kafka.SearchHeaderValue(m.Headers, hc.HeaderNames.ResumeTime))
			if resumeTimeString == "" {
				t.Fatal("Resume time header was not set")
			}

			if _, err := time.Parse(time.RFC3339, resumeTimeString); err != nil {
				t.Fatalf("Failed to parse resume time header value: %v", err)
			}

			go func() {
				c <- &ck.Message{}
			}()

			return nil
		},
	}

	failureStorage := &mockFailureStorage{
		HasFailedFn: func(s string, u uint) (bool, error) {
			return false, nil
		},
		MarkFailureFn: func(s string, u uint, id string) error {
			if msgKey != s {
				t.Fatalf("Received unexpected message key: %s != %s", msgKey, s)
			}

			if u != 0 {
				t.Fatalf("Received unexpected try counter: 0 != %d", u)
			}

			if id != getMessageId(msg) {
				t.Fatalf("Received unexpected message id: %v != %v", getMessageId(msg), id)
			}

			return nil
		},
	}

	handler := &RetryMessageHandler{
		Config:         hc,
		Producer:       kafkaProducer,
		FailureStorage: failureStorage,
		Next: MessageHandlerFunc(func(m *ck.Message) error {
			return errors.New("forced handle message error")
		}),
	}

	if err := handler.Handle(msg); err != nil {
		t.Fatal(err)
	}

	if !failureStorage.MarkFailureInvoked {
		t.Fatal("MarkFailure was not invoked")
	}

	if !kafkaProducer.ProduceInvoked {
		t.Fatal("Produce was not invoked")
	}
}

type mockFailureStorage struct {
	HasFailedFn      func(string, uint) (bool, error)
	HasFailedInvoked bool

	MarkFailureFn      func(string, uint, string) error
	MarkFailureInvoked bool

	MarkSuccessFn      func(string, string) error
	MarkSuccessInvoked bool
}

func (s *mockFailureStorage) HasFailed(key string, try uint) (bool, error) {
	s.HasFailedInvoked = true

	if s.HasFailedFn != nil {
		return s.HasFailedFn(key, try)
	} else {
		return false, nil
	}
}

func (s *mockFailureStorage) MarkFailure(key string, try uint, msgId string) error {
	s.MarkFailureInvoked = true

	if s.MarkFailureFn != nil {
		return s.MarkFailureFn(key, try, msgId)
	} else {
		return nil
	}
}

func (s *mockFailureStorage) MarkSuccess(key string, msgId string) error {
	s.MarkSuccessInvoked = true

	if s.MarkSuccessFn != nil {
		return s.MarkSuccessFn(key, msgId)
	} else {
		return nil
	}
}
