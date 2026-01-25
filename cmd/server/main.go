package main

import (
	"log"
	"lsm/internal/srv"
	"lsm/internal/srv/command/handler"
	strg "lsm/internal/storage"
	"os"
	"os/signal"
	"syscall"
)

const Port = 11211
const MaxConnections = 1000000
const ShutdownTimeout = 30

const SetBodyMaxAllowedSize = 5 * 1024 * 1024
const SetMaxConcurrentRequests = 400

const DataDir = "./../../data/"
const BlockSize = 1024 * 16
const MaxMemSize = 1024 * 1024 * 64

func main() {
	storage, err := strg.NewStorage(DataDir, BlockSize, MaxMemSize)
	if err != nil {
		panic(err)
	}

	connectionHandler := srv.NewConnectionHandler()
	connectionHandler.RegisterHandler(handler.NewGetCommandHandler(storage))
	connectionHandler.RegisterHandler(handler.NewSetCommandHandler(storage, SetBodyMaxAllowedSize, SetMaxConcurrentRequests))

	server := srv.NewServer(Port, MaxConnections, ShutdownTimeout, connectionHandler)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		err = server.Start()
		if err != nil {
			log.Println("Error during server start", err)
		}
	}()

	<-stop

	log.Println("Shutdown signal received...")

	err = server.Stop()
	if err != nil {
		log.Println("Error during server stop", err)
	}

	log.Println("Closing storage...")
	err = storage.Close()
	if err != nil {
		log.Printf("Error closing storage: %v\n", err)
	}

	log.Println("Goodbye!")
}
