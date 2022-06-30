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

	Channel *Channel // client订阅的channel

	ExitChan chan int
	lenBuf   [4]byte
	lenSlice []byte
}
