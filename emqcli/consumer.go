package emqcli

import (
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
)

var instCount int64

type Consumer struct {
	id      int64
	topic   string
	channel string
	state   int32

	conn     *Conn
	msgChan  chan *Message
	exitChan chan int

	exitOnce sync.Once
}

type Handler interface {
	HandleMessage(message *Message) error
}

func NewConsumer(topic, channel string) *Consumer {
	c := &Consumer{
		id:       atomic.AddInt64(&instCount, 1),
		topic:    topic,
		channel:  channel,
		exitChan: make(chan int),
		msgChan:  make(chan *Message),
	}
	return c
}

func (co *Consumer) Exit() {
	co.exitOnce.Do(func() {
		co.conn.Stop()
		close(co.exitChan)
	})
}

func (co *Consumer) AddHandler(handler Handler) {
	if atomic.LoadInt32(&co.state) == common.ConsumerConnected {
		panic("already connected")
	}

	go co.handlerLoop(handler)
}

func (co *Consumer) handlerLoop(handler Handler) {
	for {
		select {
		case msg := <-co.msgChan:
			err := handler.HandleMessage(msg)
			if err != nil {
				log.Infof("HandleMessage error: %v", err)
				continue
			}
		case <-co.exitChan:
			goto exit
		}
	}

exit:
	log.Infof("handlerLoop exit")
}

func (co *Consumer) ConnectToEMQD(addr string) error {
	if !atomic.CompareAndSwapInt32(&co.state, common.ConsumerInit, common.ConsumerConnected) {
		return fmt.Errorf("consumer can not connect")
	}

	co.conn = NewConn(addr, co.msgChan)
	if err := co.conn.Connect(); err != nil {
		co.Exit()
		return err
	}

	cmd := command.SubscribeCmd(co.topic, co.channel)
	if _, err := co.conn.Command(cmd); err != nil {
		co.Exit()
		return fmt.Errorf("SubscribeCmd error: %v", err)
	}

	log.Infof("ConnectToEMQD %s", addr)

	return nil
}
