package main

//ErrCode the code for current error
type ErrCode int32

const (
	//ErrSliceNotLargeEnough the slice is not large enough.
	ErrSliceNotLargeEnough ErrCode = 1

	//ErrSegmentSizeLimitReached the wal segment size limit has been reached.
	ErrSegmentSizeLimitReached = 2
)

//ErrSegLimitReached signaled when segment size limit reached.
var ErrSegLimitReached = NewWalError(ErrSegmentSizeLimitReached, "Segment limit has been reached.")

//WalError errors encapsulation.
type WalError struct {
	code    ErrCode
	message string
}

//Code Returns the actual err code.
func (we WalError) Code() ErrCode {
	return we.code
}

func (we WalError) Error() string {
	return we.message
}

//NewWalError creates a new error encapsulation.
func NewWalError(code ErrCode, msg string) error {
	err := WalError{
		code:    code,
		message: msg,
	}

	return error(err)
}
