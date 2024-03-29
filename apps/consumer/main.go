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

	consumer := emqcli.NewConsumer("topictest", "channeltest")

	consumer.AddHandler(&ConsumerHandler{})

	// 连接lookupd
	err := consumer.ConnectToLookupd("127.0.0.1:7002")
	if err != nil {
		log.Fatalf("ConnectToLookupd: %v", err)
	}
	// err = consumer.ConnectToLookupd("127.0.0.1:7012")
	// if err != nil {
	// 	log.Fatalf("ConnectToLookupd: %v", err)
	// }

	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	consumer.Stop()
}

type ConsumerHandler struct{}

func (ch *ConsumerHandler) HandleMessage(m *emqcli.Message) error {
	log.Infof(string(m.Body))
	return nil
}
