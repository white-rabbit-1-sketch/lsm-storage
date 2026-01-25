package internal_error

import "fmt"

type BaseError struct {
	Message string
	Err     error
}

func (e *BaseError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}

	return fmt.Sprintf("%s", e.Message)
}

func (e *BaseError) Unwrap() error {
	return e.Err
}
