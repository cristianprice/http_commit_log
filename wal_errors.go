package main

//ErrCode the code for current error
type ErrCode int32

const ()

//WalError errors encapsulation.
type WalError struct {
	code    ErrCode
	message string
}

//Code Returns the actual err code.
func (we *WalError) Code() ErrCode {
	return we.code
}

func (we *WalError) Error() string {
	return we.message
}

//NewWalError creates a new error encapsulation.
func NewWalError(code ErrCode, msg string) WalError {
	return WalError{
		code:    code,
		message: msg,
	}
}
