package retry

import (
	"errors"
	"strconv"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/peaceman/kafka-rnd-go/kafka"
)

type MessageHandler interface {
	Handle(*ck.Message) error
}

type MessageHandlerFunc func(*ck.Message) error

func (fn MessageHandlerFunc) Handle(m *ck.Message) error {
	return fn(m)
}

type RetryMessageHandler struct {
	Config         MessageHandlerConfig
	Producer       kafka.Producer
	FailureStorage FailureStorage
	Next           MessageHandler
}

type messageMeta struct {
	try uint
	key string
}

func (h *RetryMessageHandler) Handle(msg *ck.Message) error {
	meta := h.parseMessageMeta(msg)
	hasFailures, err := h.hasFailuresInThisTry(meta)
	if err != nil {
		return err
	}

	if hasFailures {
		return h.handleFailedMessage(msg, meta)

	} else {
		if err = h.handleMessage(msg); err != nil {
			return h.handleFailedMessage(msg, meta)
		} else {
			h.markSuccess(meta)
		}

		return nil
	}
}

func (h *RetryMessageHandler) parseMessageMeta(msg *ck.Message) messageMeta {
	var try uint = 0

	tryString := string(kafka.SearchHeaderValue(msg.Headers, h.Config.HeaderNames.Try))
	if tryString != "" {
		if t, err := strconv.ParseUint(tryString, 10, 64); err != nil {
			try = uint(t)
		}
	}

	return messageMeta{
		try: try,
		key: string(msg.Key),
	}
}

func (h *RetryMessageHandler) hasFailuresInThisTry(meta messageMeta) (bool, error) {
	return h.FailureStorage.HasFailed(meta.key, meta.try)
}

func (h *RetryMessageHandler) markFailure(meta messageMeta) error {
	return h.FailureStorage.MarkFailure(meta.key, meta.try)
}

func (h *RetryMessageHandler) republish(incMsg *ck.Message, meta messageMeta) error {
	tryHeaderName := h.Config.HeaderNames.Try
	resumeTimeHeaderName := h.Config.HeaderNames.ResumeTime

	resumeTime := time.Now().Add(time.Minute)
	resumeTimeString := resumeTime.Format(time.RFC3339)

	msg := *incMsg
	msg.TopicPartition.Topic = &h.Config.DelayTopic

	kafka.RemoveHeaders([]string{tryHeaderName, resumeTimeHeaderName}, &msg)
	msg.Headers = append(
		msg.Headers,
		ck.Header{
			Key:   tryHeaderName,
			Value: []byte(strconv.FormatUint(uint64(meta.try+1), 10)),
		},
		ck.Header{
			Key:   resumeTimeHeaderName,
			Value: []byte(resumeTimeString),
		},
	)

	deliveryChan := make(chan ck.Event)
	if err := h.Producer.Produce(&msg, deliveryChan); err != nil {
		return err
	}

	select {
	case deliveryReport := <-deliveryChan:
		deliveryMessage := deliveryReport.(*ck.Message)
		return deliveryMessage.TopicPartition.Error
	case <-time.After(h.Config.DeliveryReportTimeout):
		return errors.New("waiting for the delivery report timed out")
	}
}

func (h *RetryMessageHandler) handleMessage(msg *ck.Message) error {
	if h.Next == nil {
		return nil
	}

	return h.Next.Handle(msg)
}

func (h *RetryMessageHandler) markSuccess(meta messageMeta) error {
	return h.FailureStorage.MarkSuccess(meta.key)
}

func (h *RetryMessageHandler) handleFailedMessage(msg *ck.Message, meta messageMeta) error {
	if err := h.markFailure(meta); err != nil {
		return err
	}

	if err := h.republish(msg, meta); err != nil {
		return err
	}

	return nil
}
