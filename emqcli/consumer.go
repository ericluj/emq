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
	id               int64
	topic            string
	channel          string
	incomingMessages chan *Message
	conns            map[string]*Conn
	state            int32
	runningHandlers  int32

	mtx      sync.RWMutex
	wg       sync.WaitGroup
	exitOnce sync.Once
	exitChan chan int
}

func NewConsumer(topic, channel string) (*Consumer, error) {
	c := &Consumer{
		id:               atomic.AddInt64(&instCount, 1),
		topic:            topic,
		channel:          channel,
		incomingMessages: make(chan *Message),
		conns:            make(map[string]*Conn),
	}
	return c, nil
}

type Handler interface {
	HandleMessage(message *Message) error
}

func (co *Consumer) AddHandler(handler Handler) {
	if atomic.LoadInt32(&co.state) == stateConnected {
		panic("already connected")
	}
	atomic.AddInt32(&co.runningHandlers, 1)
	go co.handlerLoop(handler)
}

func (co *Consumer) handlerLoop(handler Handler) {
	for {
		msg, ok := <-co.incomingMessages
		if !ok { // 判断信道是否关闭
			goto exit
		}
		err := handler.HandleMessage(msg)
		if err != nil {
			continue
		}
		msg.Finish()
	}

exit:
	if atomic.AddInt32(&co.runningHandlers, -1) == 0 {
		co.exit()
	}
}

func (co *Consumer) exit() {
	co.exitOnce.Do(func() {
		close(co.exitChan)
		co.wg.Wait()
	})
}

func (co *Consumer) ConnectToEMQD(addr string) error {
	if atomic.LoadInt32(&co.state) != stateInit {
		return fmt.Errorf("consumer can not connect")
	}
	if atomic.LoadInt32(&co.runningHandlers) == 0 {
		return fmt.Errorf("no handlers")
	}
	atomic.StoreInt32(&co.state, stateConnected)

	// 创建conn，如果有不能再创建
	co.mtx.Lock()
	defer co.mtx.Unlock()
	if _, ok := co.conns[addr]; ok {
		return fmt.Errorf("already connected")
	}

	conn := NewConn(addr, &consumerConnDelegate{r: co})
	if err := conn.Connect(); err != nil {
		conn.Close()
		return err
	}

	cmd := Subscribe(co.topic, co.channel)
	if err := conn.WriteCommand(cmd); err != nil {
		conn.Close()
		return fmt.Errorf("[%v] failed to subscribe to %s:%s - %s", conn, co.topic, co.channel, err.Error())
	}

	co.conns[addr] = conn
	log.Infof("(%s) connecting to emqd", addr)

	return nil
}

func (co *Consumer) onConnMessage(c *Conn, msg *Message) {
	co.incomingMessages <- msg
}

func (co *Consumer) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (co *Consumer) onConnResponse(c *Conn, data []byte) {
	// TODO:
}

func (r *Consumer) onConnError(c *Conn, data []byte) {}

func (r *Consumer) onConnHeartbeat(c *Conn) {
	log.Infof("heartbeat")
}
