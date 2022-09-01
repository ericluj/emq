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
	for {
		msg := fmt.Sprintf("msg测试-%d", time.Now().Unix())
		log.Infof(msg)
		err = producer.Publish("topictest", msg)
		if err != nil {
			log.Fatalf("error: %v", err)
		}

		time.Sleep(time.Second)
	}
}
