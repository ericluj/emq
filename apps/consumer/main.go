package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/emqcli"
)

func main() {
	c := make(chan os.Signal, 1)

	consumer := emqcli.NewConsumer("test", "one")

	consumer.AddHandler(&ConsumerHandler{})
	err := consumer.ConnectToEMQD("127.0.0.1:6001")
	if err != nil {
		log.Fatalf("ConnectToEMQD fatal: %v", err)
	}

	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	consumer.Exit()
}

type ConsumerHandler struct{}

func (ch *ConsumerHandler) HandleMessage(m *emqcli.Message) error {
	log.Infof(string(m.Body))
	return nil
}
