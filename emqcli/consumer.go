package emqcli

import (
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
)

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

var instCount int64

type Consumer struct {
	id                int64
	topic             string
	channel           string
	incommingMessages chan *Message
	conns             map[string]*Conn
	wg                sync.WaitGroup
	state             int32
	runningHandlers   int32
	sync.RWMutex
}

func NewConsumer(topic, channel string) (*Consumer, error) {
	c := &Consumer{
		id:                atomic.AddInt64(&instCount, 1),
		topic:             topic,
		channel:           channel,
		incommingMessages: make(chan *Message),
		conns:             make(map[string]*Conn),
	}
	return c, nil
}

type Handler interface {
	HandleMessage(message *Message) error
}

func (c *Consumer) AddHandler(handler Handler) {
	if atomic.LoadInt32(&c.state) == stateConnected {
		panic("already connected")
	}
	atomic.AddInt32(&c.runningHandlers, 1)
	go c.handlerLoop(handler)
}

func (c *Consumer) handlerLoop(handler Handler) {
	for {
		msg, ok := <-c.incommingMessages
		if !ok {
			goto exit
		}
		err := handler.HandleMessage(msg)
		if err != nil {
			continue
		}
		msg.Finish()
	}

exit:
	if atomic.AddInt32(&c.runningHandlers, -1) == 0 {
		c.exit()
	}
}

func (c *Consumer) exit() {

}

func (c *Consumer) ConnectToEMQD(addr string) error {
	if atomic.LoadInt32(&c.state) != stateInit {
		return fmt.Errorf("consumer can not connect")
	}
	if atomic.LoadInt32(&c.runningHandlers) == 0 {
		return fmt.Errorf("no handlers")
	}
	atomic.StoreInt32(&c.state, stateConnected)

	conn := NewConn(addr)
	c.Lock()
	_, ok := c.conns[addr]
	if ok {
		c.Unlock()
		return fmt.Errorf("already connected")
	}
	c.conns[addr] = conn
	c.Unlock()
	log.Infof("(%s) connecting to emqd", addr)

	cleanupConnection := func() {
		c.Lock()
		delete(c.conns, addr)
		c.Unlock()
		conn.Close()
	}

	err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	cmd := Subscribe(c.topic, c.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%v] failed to subscribe to %s:%s - %s", conn, c.topic, c.channel, err.Error())
	}

	return nil
}
