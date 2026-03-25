package main

import (
	"log"

	"erionn-mq/internal/amqp"
	"erionn-mq/internal/config"
	"erionn-mq/internal/core"
	"erionn-mq/internal/management"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	broker, err := core.NewDurableBroker(cfg.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	server := amqp.NewServer(cfg.AMQPAddr, broker)
	log.Printf("AMQP 0-9-1 server listening on %s", server.Addr)
	mgmt := management.NewServerWithConfig(management.Config{
		Addr:        cfg.ManagementAddr,
		Users:       cfg.ManagementUsers,
		AllowRemote: cfg.ManagementAllowRemote,
	}, broker, server)
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
