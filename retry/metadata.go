package retry

type FailureStorage interface {
	HasFailed(msgKey string, try uint) (bool, error)
	MarkFailure(msgKey string, try uint, msgId string) error
	MarkSuccess(msgKey string, msgId string) error
}
