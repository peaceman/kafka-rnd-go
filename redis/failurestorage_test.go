package redis

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-redis/redismock/v8"
)

func TestFailureStorage_KeyPrefix(t *testing.T) {
	storage, mock := setupFailureStorage()
	prefix := "prefix"
	storage.Config = &RedisFailureStorageConfig{
		KeyPrefix: prefix,
	}

	t.Run("HasFailed", func(t *testing.T) {
		mock.ExpectSCard(rkeyPrefix("foo", 1, prefix)).SetVal(0)
		storage.HasFailed("foo", 1)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("MarkFailure", func(t *testing.T) {
		mock.ExpectSAdd(rkeyPrefix("foo", 1, prefix), "id")
		storage.MarkFailure("foo", 1, "id")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("MarkSuccess", func(t *testing.T) {
		mock.ExpectExists(rkeyPrefix("foo", 0, prefix)).SetVal(0)
		storage.MarkSuccess("foo", "id")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

func TestFailureStorage_HasFailed(t *testing.T) {
	storage, mock := setupFailureStorage()

	t.Run("failures", func(t *testing.T) {
		mock.ExpectSCard(rkey("foo", 1)).SetVal(0)
		result, _ := storage.HasFailed("foo", 1)
		if result != false {
			t.Fatal("Expected false")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("no failures", func(t *testing.T) {
		mock.ExpectSCard(rkey("foo", 1)).SetVal(3)
		result, _ := storage.HasFailed("foo", 1)
		if result != true {
			t.Fatal("Expected true")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("error propagation", func(t *testing.T) {
		mock.ExpectSCard(rkey("foo", 1)).SetErr(errors.New("forced error"))
		_, err := storage.HasFailed("foo", 1)
		if err == nil {
			t.Fatal("Expected propagated redis error")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

func TestFailureStorage_MarkFailure(t *testing.T) {
	storage, mock := setupFailureStorage()
	var msgKey, msgId, msgTry = "foo", "id", uint(1)

	t.Run("regular", func(t *testing.T) {
		mock.ExpectSAdd(rkey(msgKey, msgTry), msgId).SetVal(2)

		err := storage.MarkFailure(msgKey, msgTry, msgId)
		if err != nil {
			t.Error(err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("error", func(t *testing.T) {
		mock.ExpectSAdd(rkey(msgKey, msgTry), msgId).SetErr(errors.New("forced error"))

		err := storage.MarkFailure(msgKey, msgTry, msgId)
		if err == nil {
			t.Fatal("Expected propagated redis error")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

func TestFailureStorage_MarkSuccess(t *testing.T) {
	storage, mock := setupFailureStorage()
	var msgKey, msgId = "foo", "id"

	t.Run("regular", func(t *testing.T) {
		mock.ExpectExists(rkey(msgKey, 0)).SetVal(1)
		mock.ExpectExists(rkey(msgKey, 1)).SetVal(1)
		mock.ExpectExists(rkey(msgKey, 2)).SetVal(0)

		mock.ExpectSRem(rkey(msgKey, 0), msgId).SetVal(1)
		mock.ExpectSRem(rkey(msgKey, 1), msgId).SetVal(1)

		err := storage.MarkSuccess(msgKey, msgId)
		if err != nil {
			t.Error(err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

func setupFailureStorage() (*RedisFailureStorage, redismock.ClientMock) {
	db, mock := redismock.NewClientMock()
	storage := &RedisFailureStorage{
		Redis: db,
	}

	return storage, mock
}

func rkey(msgKey string, try uint) string {
	return rkeyPrefix(msgKey, try, "")
}

func rkeyPrefix(msgKey string, try uint, prefix string) string {
	key := fmt.Sprintf("krnd:message-failures:%s-%d", msgKey, try)

	return strings.Join(removeEmptyString([]string{prefix, key}), ":")
}

func removeEmptyString(in []string) []string {
	out := make([]string, 0, len(in))

	for _, v := range in {
		if v != "" {
			out = append(out, v)
		}
	}

	return out
}
