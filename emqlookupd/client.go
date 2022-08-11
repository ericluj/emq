package emqlookupd

import (
	"net"
)

type Client struct {
	conn     net.Conn
	peerInfo *PeerInfo
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
