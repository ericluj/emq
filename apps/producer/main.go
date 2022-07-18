package main

import (
	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/emqcli"
)

func main() {
	producer, err := emqcli.NewProducer("127.0.0.1:6001")
	if err != nil {
		log.Fatalf("NewProducer fatal: %v", err)
	}
	producer.Publish("test", "ceshi 测试")
}
