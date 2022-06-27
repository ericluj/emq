package emqd

import (
	"net"
	"sync/atomic"
)

// 协议结构体 用来组织emqd和client的关联处理
type Protocol struct {
	emqd *EMQD
}

func (p *Protocol) NewClient(conn net.Conn) *Client {
	clientID := atomic.AddInt64(&p.emqd.clientIDSequence, 1)
	c := &Client{
		ID: clientID,
	}
	return c
}

func (p *Protocol) IOLoop(c *Client) error {
	for {

	}
}
