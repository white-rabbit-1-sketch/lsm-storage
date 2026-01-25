package internal_error

type ClientError struct {
	BaseError
}

func NewClientError(msg string, err error) *ClientError {
	return &ClientError{
		BaseError{
			Message: msg,
			Err:     err,
		},
	}
}
