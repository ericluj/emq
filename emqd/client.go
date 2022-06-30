package emqd

import (
	"bufio"
	"net"
)

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type Client struct {
	ID   int64
	emqd *EMQD
	net.Conn

	Reader *bufio.Reader
	Writer *bufio.Writer

	Channel *Channel // client订阅的channel
	State   int32

	ExitChan chan int
	lenBuf   [4]byte
	lenSlice []byte
}
