package mock

import ck "github.com/confluentinc/confluent-kafka-go/kafka"

type KafkaProducer struct {
	CloseFn      func()
	CloseInvoked bool

	ProduceFn      func(*ck.Message, chan ck.Event) error
	ProduceInvoked bool
}

func (p *KafkaProducer) Close() {
	p.CloseInvoked = true

	if p.CloseFn != nil {
		p.CloseFn()
	}
}

func (p *KafkaProducer) Produce(m *ck.Message, reportChan chan ck.Event) error {
	p.ProduceInvoked = true

	if p.ProduceFn != nil {
		return p.ProduceFn(m, reportChan)
	} else {
		go func() {
			reportChan <- &ck.Message{
				TopicPartition: ck.TopicPartition{},
			}
		}()

		return nil
	}
}
