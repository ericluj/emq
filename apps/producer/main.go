package main

import (
	"fmt"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/emqcli"
)

func main() {
	producer, err := emqcli.NewProducer("127.0.0.1:6001")
	if err != nil {
		log.Fatalf("NewProducer fatal: %v", err)
	}
	msg := fmt.Sprintf("msg测试%d", time.Now().Unix())
	err = producer.Publish("topictest", msg)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	log.Infof(msg)
	log.Infof("end")
}
