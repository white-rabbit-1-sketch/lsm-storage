package handler

import "bufio"

type Handler interface {
	Handle(
		reader *bufio.Reader,
		writer *bufio.Writer,
		parts []string,
	) error
	Name() string
}
