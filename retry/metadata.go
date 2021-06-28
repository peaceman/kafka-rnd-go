package retry

type FailureStorage interface {
	HasFailed(key string, try uint) (bool, error)
	MarkFailure(key string, try uint, msgId string) error
	MarkSuccess(key string, msgId string) error
}
