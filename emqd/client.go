package emqd

import (
	"bufio"
	"net"
)

type Client struct {
	ID   int64
	emqd *EMQD
	net.Conn

	Reader *bufio.Reader
	Writer *bufio.Writer

	ExitChan chan int
}
