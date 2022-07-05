package emqcli

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	log "github.com/ericluj/elog"
)

var (
	MagicV1 = []byte("  V1")
	timeout = time.Second * 5
)

type Conn struct {
	addr string
	conn *net.TCPConn
	r    io.Reader
	w    io.Writer
	sync.RWMutex
}

func NewConn(addr string) *Conn {
	return &Conn{
		addr: addr,
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(timeout))
	return c.w.Write(p)
}

func (c *Conn) Close() {
	c.conn.CloseRead()
}

func (c *Conn) Connect() error {
	dialer := &net.Dialer{
		Timeout: timeout,
	}

	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return err
	}

	c.conn = conn.(*net.TCPConn)
	c.r = conn
	c.w = conn

	_, err = c.Write(MagicV1)
	if err != nil {
		c.Close()
		return fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	go c.readLoop()
	go c.writeLoop()

	return nil
}

func (c *Conn) readLoop() {

}

func (c *Conn) writeLoop() {

}

func (c *Conn) WriteCommand(cmd *Command) error {
	c.Lock()
	defer c.Unlock()
	_, err := cmd.WriteTo(c)
	if err != nil {
		log.Infof("WriteCommand error: %v, cmd: %v", err, cmd)
		return err
	}
	return nil
}
