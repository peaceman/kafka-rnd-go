package kafka

import (
	ck "github.com/confluentinc/confluent-kafka-go/kafka"
)

func SearchHeaderValue(headers []ck.Header, key string) []byte {
	for _, h := range headers {
		if h.Key == key {
			return h.Value
		}
	}

	return nil
}

func RemoveHeaders(headerNames []string, msg *ck.Message) {
	set := make(map[string]interface{}, len(msg.Headers))
	for _, n := range headerNames {
		set[n] = nil
	}

	fh := make([]ck.Header, 0, len(msg.Headers))

	for _, header := range msg.Headers {
		if _, ok := set[header.Key]; !ok {
			fh = append(fh, header)
		}
	}

	msg.Headers = fh
}

func IsReadTimeout(msg *ck.Message, err error) bool {
	if err == nil {
		return false
	}

	kafkaError, ok := err.(ck.Error)
	if !ok {
		return false
	}

	return kafkaError.Code() == ck.ErrTimedOut
}
