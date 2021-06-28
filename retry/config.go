package retry

import "time"

type Config struct {
	PrimaryTopics   []string
	DelayTopic      string
	ConsumerGroupId string
	HeaderNames     HeaderNameConfig
}

type MessageHandlerConfig struct {
	Config
	DeliveryReportTimeout time.Duration
}

type HeaderNameConfig struct {
	ResumeTime  string
	TargetTopic string
	Try         string
}
