package main

import (
	"log"

	"erionn-mq/internal/amqp"
	"erionn-mq/internal/amqpcore"
	"erionn-mq/internal/store"
)

func main() {
	broker := amqpcore.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})

	server := amqp.NewServer(amqp.DefaultAddr, broker)
	log.Printf("AMQP 0-9-1 server listening on %s", server.Addr)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
