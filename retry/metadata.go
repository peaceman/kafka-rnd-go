package retry

type FailureStorage interface {
	HasFailed(key string, try uint) (bool, error)
	MarkFailure(key string, try uint) error
	MarkSuccess(key string) error
}