package srv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"lsm/internal/srv/command/handler"
	"lsm/internal/srv/internal_error"
	"net"
	"strings"
	"time"
)

const ReadTimeout = 30

type ConnectionHandler struct {
	commandHandlers map[string]handler.Handler
}

func NewConnectionHandler() *ConnectionHandler {
	return &ConnectionHandler{
		commandHandlers: make(map[string]handler.Handler),
	}
}

func (h *ConnectionHandler) RegisterHandler(hndlr handler.Handler) {
	h.commandHandlers[hndlr.Name()] = hndlr
}

func (h *ConnectionHandler) handle(conn net.Conn) error {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		err := conn.SetReadDeadline(time.Now().Add(time.Second * ReadTimeout))
		if err != nil {
			return err
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			_, err = conn.Write([]byte(fmt.Sprintf("(empty command)\r\n", err)))
			if err != nil {
				return err
			}

			continue
		}

		cmd := strings.ToUpper(parts[0])
		hndlr, ok := h.commandHandlers[cmd]
		if !ok {
			_, err = conn.Write([]byte(fmt.Sprintf("(invalid command)\r\n", err)))
			if err != nil {
				return err
			}

			continue
		}

		err = hndlr.Handle(reader, writer, parts)
		if err != nil {
			var clientErr *internal_error.ClientError
			if errors.As(err, &clientErr) {
				_, err = conn.Write([]byte(fmt.Sprintf("(%s)\r\n", err)))
				if err != nil {
					return err
				}

				continue
			}

			return err
		}
	}
}
