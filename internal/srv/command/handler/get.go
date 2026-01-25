package handler

import (
	"bufio"
	"lsm/internal/srv/internal_error"
	strg "lsm/internal/storage"
	"strconv"
)

const getCommandName = "GET"

var (
	respValue = []byte("VALUE ")
	respEnd   = []byte("\r\nEND\r\n")
	respEmpty = []byte("(empty)\r\n")
	space     = []byte(" ")
	crlf      = []byte("\r\n")
)

type GetCommandHandler struct {
	storage *strg.Storage
}

func NewGetCommandHandler(storage *strg.Storage) *GetCommandHandler {
	return &GetCommandHandler{
		storage: storage,
	}
}

func (h *GetCommandHandler) Name() string {
	return getCommandName
}

func (h *GetCommandHandler) Handle(
	reader *bufio.Reader,
	writer *bufio.Writer,
	parts []string,
) error {
	defer writer.Flush()

	if len(parts) < 2 || parts[1] == "" {
		return internal_error.NewClientError("missing arguments", nil)
	}

	data, flags, found, err := h.storage.Get(parts[1])
	if err != nil {
		return err
	}

	if !found {
		_, err = writer.Write(respEmpty)
		return err
	}

	var numBuf [20]byte

	// "VALUE "
	_, err = writer.Write(respValue)
	if err != nil {
		return err
	}

	// 2. "<key> "
	_, err = writer.WriteString(parts[1])
	if err != nil {
		return err
	}

	_, err = writer.Write(space)
	if err != nil {
		return err
	}

	// 3. "<flags> "
	_, err = writer.Write(strconv.AppendUint(numBuf[:0], uint64(flags), 10))
	if err != nil {
		return err
	}
	_, err = writer.Write(space)
	if err != nil {
		return err
	}

	// 4. "<bytes>\r\n"
	_, err = writer.Write(strconv.AppendUint(numBuf[:0], uint64(len(data)), 10))
	if err != nil {
		return err
	}
	_, err = writer.Write(crlf)
	if err != nil {
		return err
	}

	// 5. <data>
	_, err = writer.Write(data)
	if err != nil {
		return err
	}

	// 6. "\r\nEND\r\n"
	_, err = writer.Write(respEnd)
	if err != nil {
		return err
	}

	return nil
}
