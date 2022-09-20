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
		log.Fatalf("NewProducer: %v", err)
	}
	producer2, err := emqcli.NewProducer("127.0.0.1:6011")
	if err != nil {
		log.Fatalf("NewProducer: %v", err)
	}
	for {
		msg := fmt.Sprintf("msg测试-%d", time.Now().Unix())
		log.Debugf(msg)
		err = producer.Publish("topictest", msg)
		if err != nil {
			log.Fatalf("Publish: %v", err)
		}

		msg2 := fmt.Sprintf("msg测试2-%d", time.Now().Unix())
		log.Debugf(msg)
		err = producer2.Publish("topictest", msg2)
		if err != nil {
			log.Fatalf("Publish: %v", err)
		}

		time.Sleep(time.Second)
	}
}
