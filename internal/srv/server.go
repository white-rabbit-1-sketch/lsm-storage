package srv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

type Server struct {
	ctx               context.Context
	cancel            context.CancelFunc
	ctxMutex          sync.Mutex
	wg                sync.WaitGroup
	listener          net.Listener
	listenerMutex     sync.Mutex
	connectionHandler *ConnectionHandler
	port              int
	maxConnections    int
	shutdownTimeout   int
}

func NewServer(
	port int,
	maxConnections int,
	shutdownTimeout int,
	connectionHandler *ConnectionHandler,
) *Server {
	return &Server{
		port:              port,
		maxConnections:    maxConnections,
		shutdownTimeout:   shutdownTimeout,
		connectionHandler: connectionHandler,
	}
}

func (s *Server) Start() error {
	if s.listener != nil {
		return fmt.Errorf("server already started")
	}

	s.ctxMutex.Lock()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.ctxMutex.Unlock()

	var err error
	s.listenerMutex.Lock()
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	s.listenerMutex.Unlock()
	if err != nil {
		return err
	}

	log.Printf("Server started at :%d\n", s.port)

	err = s.accept()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) accept() error {
	semaphore := make(chan struct{}, s.maxConnections)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(s.ctx.Err(), context.Canceled) {
				return nil
			}

			log.Printf("Error during connection accept %v", err)

			continue
		}

		semaphore <- struct{}{}

		s.wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Panic recovered: %v\n%s", r, debug.Stack())
				}

				conn.Close()
				<-semaphore
			}()

			err = s.connectionHandler.handle(conn)
			if err != nil {
				log.Printf("Error during connection handling %v", err)
			}
		})
	}
}

func (s *Server) Stop() error {
	s.ctxMutex.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.ctxMutex.Unlock()

	s.listenerMutex.Lock()
	if s.listener != nil {
		log.Println("Stopping server...")
		s.listener.Close()
	}
	s.listenerMutex.Unlock()

	err := s.waitForShutdown()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) waitForShutdown() error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(time.Second * time.Duration(s.shutdownTimeout)):
		return fmt.Errorf("shutdown timeout")
	}
}
