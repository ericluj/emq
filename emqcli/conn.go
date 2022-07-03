package emqcli

import "net"

type Conn struct {
	addr string
	conn *net.TCPConn
}

func NewConn(addr string) *Conn {
	return &Conn{
		addr: addr,
	}
}

func (c *Conn) Connect() error {
	return nil
}
