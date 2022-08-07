package emqcli

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/util"
)

type msgResponse struct {
	// msg *Message
	// cmd *command.Command
}

type Conn struct {
	addr     string
	conn     *net.TCPConn
	delegate ConnDelegate

	r io.Reader
	w io.Writer

	cmdChan         chan *command.Command
	msgResponseChan chan *msgResponse

	mtx       sync.RWMutex
	wg        util.WaitGroup
	closeFlag int32
	exitOnce  sync.Once
	exitChan  chan int
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
	return c.w.Write(p)
}

func (c *Conn) Read(p []byte) (int, error) {
	err := c.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
	if err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func (c *Conn) Close() error {
	atomic.StoreInt32(&c.closeFlag, 1)
	if c.conn != nil {
		err := c.conn.CloseRead()
		if err != nil {
			return err
		}
	}

	c.wg.Wait()
	return nil
}

func (c *Conn) Connect() error {
	dialer := &net.Dialer{
		Timeout: common.DialTimeout,
	}

	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return err
	}

	c.conn = conn.(*net.TCPConn)
	c.r = conn
	c.w = conn

	_, err = c.Write([]byte(common.ProtoMagic))
	if err != nil {
		c.Close()
		return fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	err = c.identify()
	if err != nil {
		return err
	}

	c.wg.Wrap(c.readLoop)
	c.wg.Wrap(c.writeLoop)

	return nil
}

func (c *Conn) identify() error {
	// if err := c.upgradeTLS(); err != nil {
	// 	return err
	// }

	return nil
}

func (c *Conn) readLoop() {
	for {
		// 已经被关闭
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}

		// 读取数据
		frameType, data, err := ReadUnpackedResponse(c)
		if err != nil {
			// 读到了结束且连接关闭
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 {
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
			err := c.WriteCommand(command.NopCmd())
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
	c.close() // TODO:这个关闭会把writeLoop关掉，为什么要有两个呢
	log.Infof("readLoop exiting")
}

func (c *Conn) writeLoop() {

}

func (c *Conn) close() {
	c.exitOnce.Do(func() {
		log.Infof("beginning close")
		close(c.exitChan)
		c.conn.CloseRead()
	})

}

func (c *Conn) WriteCommand(cmd *command.Command) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if _, err := cmd.WriteTo(c); err != nil {
		log.Infof("WriteCommand error: %v, cmd: %v", err, cmd)
		return err
	}
	return nil
}
