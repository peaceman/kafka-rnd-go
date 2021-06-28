package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

// Stores failure information in redis in form of sets that contain the message ids of
// failed messages. These sets are keyed by $prefix:krnd:message-failures:$msgKey-$try
type RedisFailureStorage struct {
	Redis  *redis.Client
	Config *RedisFailureStorageConfig
}

type RedisFailureStorageConfig struct {
	KeyPrefix string
}

func (s *RedisFailureStorage) HasFailed(msgKey string, try uint) (bool, error) {
	c, err := s.Redis.SCard(ctx, s.key(msgKey, try)).Result()
	if err != nil {
		return false, err
	}

	return c != 0, nil
}

func (s *RedisFailureStorage) MarkFailure(msgKey string, try uint, msgId string) error {
	err := s.Redis.SAdd(ctx, s.key(msgKey, try), msgId).Err()
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisFailureStorage) MarkSuccess(msgKey string, msgId string) error {
	maxTry, err := s.getMaxTry(msgKey)
	if err != nil {
		return err
	}

	for try := uint(0); try <= maxTry; try++ {
		if err := s.Redis.SRem(ctx, s.key(msgKey, try), msgId).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (s *RedisFailureStorage) key(msgKey string, try uint) string {
	return strings.TrimLeft(fmt.Sprintf(
		"%s:krnd:message-failures:%s-%d",
		s.keyPrefix(),
		msgKey,
		try,
	), ":")
}

func (s *RedisFailureStorage) keyPrefix() string {
	if s.Config != nil {
		return s.Config.KeyPrefix
	}

	return ""
}

func (s *RedisFailureStorage) getMaxTry(msgKey string) (uint, error) {
	for maxTry := uint(0); ; maxTry++ {
		exists, err := s.Redis.Exists(ctx, s.key(msgKey, maxTry)).Result()
		if err != nil {
			return 0, err
		}

		if exists == 0 {
			return maxTry - 1, nil
		}
	}
}
