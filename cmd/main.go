package main

import (
	"log"
	"path/filepath"

	"erionn-mq/internal/amqp"
	"erionn-mq/internal/amqpcore"
	"erionn-mq/internal/management"
)

func main() {
	broker, err := amqpcore.NewDurableBroker(filepath.Join("data", "broker"))
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	server := amqp.NewServer(amqp.DefaultAddr, broker)
	log.Printf("AMQP 0-9-1 server listening on %s", server.Addr)
	mgmt := management.NewServer(management.DefaultAddr, broker, server)
	log.Printf("Management API listening on %s", mgmt.Addr)
	go func() {
		if err := mgmt.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
