package emqd

import (
	"net"
	"sync"
)

type Client struct {
	ID      int64
	Channel *Channel // client订阅的channel

	conn         net.Conn
	SubEventChan chan *Channel // 事件，说明client有订阅
	State        int32
	writeLock    sync.RWMutex
	ExitChan     chan int
	lenSlice     []byte
}

func NewClient(id int64, conn net.Conn) *Client {
	c := &Client{
		ID:           id,
		conn:         conn,
		ExitChan:     make(chan int),
		SubEventChan: make(chan *Channel),
	}
	c.lenSlice = make([]byte, 4)
	return c
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type identifyData struct {
}
