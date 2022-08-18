package emqcli

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/ericluj/emq/internal/util"
)

type msgResponse struct {
	// msg *Message
	// cmd *command.Command
}

type Conn struct {
	mtx  sync.RWMutex
	wg   util.WaitGroup
	conn net.Conn

	addr     string
	delegate ConnDelegate

	cmdChan         chan *command.Command
	msgResponseChan chan *msgResponse
	exitChan        chan int
}

func NewConn(addr string, delegate ConnDelegate) *Conn {
	return &Conn{
		addr:     addr,
		delegate: delegate,

		cmdChan:         make(chan *command.Command),
		msgResponseChan: make(chan *msgResponse),

		exitChan: make(chan int),
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	err := c.conn.SetWriteDeadline(time.Now().Add(common.WriteTimeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Write(p)
}

func (c *Conn) Read(p []byte) (int, error) {
	err := c.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Read(p)
}

func (c *Conn) Stop() {
	if c.conn != nil {
		c.conn.Close()
	}

	close(c.exitChan)

	c.wg.Wait()
}

func (c *Conn) Connect() error {
	// tcp连接
	conn, err := net.DialTimeout("tcp", c.addr, common.DialTimeout)
	if err != nil {
		return err
	}
	c.conn = conn

	// 协议版本
	_, err = c.Write([]byte(common.ProtoMagic))
	if err != nil {
		c.conn.Close()
		return err
	}

	// identify
	identifyData := map[string]interface{}{}
	cmd, err := command.IDENTIFYCmd(identifyData)
	if err != nil {
		c.conn.Close()
		return err
	}
	_, err = c.Command(cmd)
	if err != nil {
		return err
	}

	c.wg.Wrap(c.readLoop)
	c.wg.Wrap(c.writeLoop)

	return nil
}

func (c *Conn) readLoop() {
	for {
		select {
		case <-c.exitChan:
			goto exit
		default:
		}

		// 读取数据
		frameType, data, err := protocol.ReadFrameData(c.conn)
		if err != nil {
			// 读到了结束且连接关闭
			if err == io.EOF {
				goto exit
			}
			// 其他错误
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Infof("IO error: %v", err)
				c.delegate.OnIOError(c, err)
			}
			goto exit
		}

		// 心跳处理
		if frameType == common.FrameTypeResponse && bytes.Equal(data, common.HeartbeatBytes) {
			c.delegate.OnHeartbeat(c)
			_, err := c.Command(command.NopCmd())
			if err != nil {
				log.Infof("IO error: %v", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			continue
		}

		switch frameType {
		case common.FrameTypeResponse:
			c.delegate.OnResponse(c, data)
		case common.FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				log.Infof("IO error: %v", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			msg.EMQDAddress = c.addr
			c.delegate.OnMessage(c, msg)
		case common.FrameTypeError:
			log.Infof("protocol error: %v", err)
			c.delegate.OnError(c, data)
		default:
			e := fmt.Errorf("unknown frame type %d", frameType)
			log.Infof("IO error: %s", e)
			c.delegate.OnIOError(c, e)
		}
	}

exit:
	log.Infof("readLoop exiting")
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.exitChan:
			return
		default:
			log.Infof("default")
		}
	}
}

func (c *Conn) Command(cmd *command.Command) ([]byte, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if err := cmd.Write(c.conn); err != nil {
		return nil, err
	}
	resp, err := protocol.ReadData(c.conn)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
