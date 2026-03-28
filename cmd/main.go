package main

import (
	"log"

	"erionn-mq/internal/amqp"
	"erionn-mq/internal/config"
	"erionn-mq/internal/broker"
	"erionn-mq/internal/management"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	b, err := broker.NewDurableBroker(cfg.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	server := amqp.NewServer(cfg.AMQPAddr, b)
	log.Printf("AMQP 0-9-1 server listening on %s", server.Addr)
	mgmt := management.NewServerWithConfig(management.Config{
		Addr:        cfg.ManagementAddr,
		Users:       cfg.ManagementUsers,
		AllowRemote: cfg.ManagementAllowRemote,
	}, b, server)
	log.Printf("Management API listening on %s", cfg.ManagementAddr)
	go func() {
		if err := mgmt.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
