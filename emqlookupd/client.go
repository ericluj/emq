package emqlookupd

import (
	"bufio"
	"net"
)

type Client struct {
	net.Conn
	Reader   *bufio.Reader
	Writer   *bufio.Writer
	peerInfo *PeerInfo
}
