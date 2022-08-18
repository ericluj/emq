package emqd

import (
	"bufio"
	"net"
	"sync"

	"github.com/ericluj/emq/internal/common"
)

type Client struct {
	ID  int64
	mtx sync.RWMutex

	conn    net.Conn
	state   int32
	channel *Channel      // client订阅的channel
	reader  *bufio.Reader // 包装io流，提升性能

	subEventChan chan *Channel // 事件，说明client有订阅
	exitChan     chan int
}

func NewClient(id int64, conn net.Conn) *Client {
	c := &Client{
		ID:           id,
		conn:         conn,
		reader:       bufio.NewReaderSize(conn, common.DefaultBufferSize),
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
