package emqlookupd

import (
	"bufio"
	"net"

	"github.com/ericluj/emq/internal/common"
)

type Client struct {
	conn     net.Conn
	reader   *bufio.Reader // 包装io流，提升性能
	peerInfo *PeerInfo
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, common.DefaultBufferSize),
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
