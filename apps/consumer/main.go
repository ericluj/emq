package main

import (
	"os"

	"emq/emqcli"

	log "github.com/ericluj/elog"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	consumer, err := emqcli.NewConsumer("test", "one")
	if err != nil {
		log.Fatalf("NewConsumer fatal: %v", err)
	}
	consumer.AddHandler(&ConsumerHandler{})

	err = consumer.ConnectToEMQD("127.0.0.1:6001")
	if err != nil {
		log.Fatalf("ConnectToEMQD fatal: %v", err)
	}
	<-sigChan
}

type ConsumerHandler struct{}

func (ch *ConsumerHandler) HandleMessage(m *emqcli.Message) error {
	log.Infof(string(m.Body))
	return nil
}
