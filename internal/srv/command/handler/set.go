package handler

import (
	"bufio"
	"io"
	"lsm/internal/srv/internal_error"
	strg "lsm/internal/storage"
	"strconv"
	"sync"
)

const setCommandName = "SET"

var respStored = []byte("STORED\r\n")

type SetCommandHandler struct {
	storage               *strg.Storage
	bodyBufferPool        sync.Pool
	semaphore             chan struct{}
	bodyMaxAllowedSize    int
	maxConcurrentRequests int
}

func NewSetCommandHandler(
	storage *strg.Storage,
	bodyMaxAllowedSize int,
	maxConcurrentRequests int,
) *SetCommandHandler {
	return &SetCommandHandler{
		storage: storage,
		bodyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, bodyMaxAllowedSize)
			},
		},
		semaphore:             make(chan struct{}, maxConcurrentRequests),
		bodyMaxAllowedSize:    bodyMaxAllowedSize,
		maxConcurrentRequests: maxConcurrentRequests,
	}
}

func (h *SetCommandHandler) Name() string {
	return setCommandName
}

func (h *SetCommandHandler) Handle(
	reader *bufio.Reader,
	writer *bufio.Writer,
	parts []string,
) error {
	defer writer.Flush()

	if len(parts) < 5 {
		return internal_error.NewClientError("missing arguments", nil)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return internal_error.NewClientError("invalid flags", err)
	}

	bytesLen, err := strconv.Atoi(parts[4])
	if err != nil {
		return internal_error.NewClientError("invalid length", nil)
	}

	if bytesLen > h.bodyMaxAllowedSize {
		return internal_error.NewClientError("value is too large (max 5MB)", nil)
	}

	h.semaphore <- struct{}{}
	fullBuf := h.bodyBufferPool.Get().([]byte)
	defer h.bodyBufferPool.Put(fullBuf)
	defer func() { <-h.semaphore }()

	data := fullBuf[:bytesLen]
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return err
	}

	_, err = reader.Discard(2)
	if err != nil {
		return err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	err = h.storage.Set(parts[1], dataCopy, uint32(flags))
	if err != nil {
		return err
	}

	_, err = writer.Write(respStored)
	return err
}
