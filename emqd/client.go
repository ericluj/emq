package emqd

import (
	"net"
	"sync"
)

type Client struct {
	ID int64

	conn net.Conn

	Channel      *Channel      // client订阅的channel
	SubEventChan chan *Channel // 事件，说明client有订阅
	State        int32

	writeLock sync.RWMutex

	ExitChan chan int
	lenBuf   [4]byte
	lenSlice []byte
}

type identifyData struct {
}

func (c *Client) Close() error {
	return c.conn.Close()
}
