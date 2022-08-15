package emqd

import (
	"net"
	"sync"
)

type Client struct {
	ID  int64
	mtx sync.RWMutex

	conn    net.Conn
	state   int32
	channel *Channel // client订阅的channel

	subEventChan chan *Channel // 事件，说明client有订阅
	exitChan     chan int
}

func NewClient(id int64, conn net.Conn) *Client {
	c := &Client{
		ID:           id,
		conn:         conn,
		exitChan:     make(chan int),
		subEventChan: make(chan *Channel),
	}
	return c
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type IdentifyData struct {
}
