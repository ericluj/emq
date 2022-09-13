package emqcli

import (
	"bytes"
	"net"
	"sync"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/ericluj/emq/internal/util"
)

type Conn struct {
	mtx  sync.RWMutex
	wg   util.WaitGroup
	conn net.Conn

	addr    string
	msgChan chan *Message

	exitChan chan int
	delegate Delegate
}

func NewConn(addr string, msgChan chan *Message, delegate Delegate) *Conn {
	return &Conn{
		addr:     addr,
		msgChan:  msgChan,
		exitChan: make(chan int),
		delegate: delegate,
	}
}

func (c *Conn) write(p []byte) (int, error) {
	err := c.conn.SetWriteDeadline(time.Now().Add(common.WriteTimeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Write(p)
}

// 主动停止
func (c *Conn) Stop() {
	if c.conn != nil {
		c.conn.Close()
	}

	close(c.exitChan)

	c.wg.Wait()

	c.delegate.OnClose(c)
}

func (c *Conn) Connect() error {
	// tcp连接
	conn, err := net.DialTimeout("tcp", c.addr, common.DialTimeout)
	if err != nil {
		return err
	}
	c.conn = conn

	// 协议版本
	_, err = c.write([]byte(common.ProtoMagic))
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
	err = c.Command(cmd)
	if err != nil {
		return err
	}
	// TODO: 返回数据处理

	c.wg.Wrap(c.readLoop)

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
		err := c.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
		if err != nil {
			log.Errorf("SetReadDeadline: %v", err)
			goto exit
		}
		frameType, body, err := protocol.ReadFrameData(c.conn)
		if err != nil {
			log.Errorf("ReadFrameData: %v", err)
			goto exit

		}

		// 心跳处理
		if frameType == common.FrameTypeResponse && bytes.Equal(body, common.HeartbeatBytes) {
			err := c.Command(command.NopCmd())
			if err != nil {
				log.Errorf("Command: %v", err)
				goto exit
			}
			continue
		}

		switch frameType {
		case common.FrameTypeResponse:
			log.Infof("FrameTypeResponse: %s", string(body))
		case common.FrameTypeMessage:
			m, err := protocol.DecodeMessage(body)
			if err != nil {
				log.Errorf("DecodeMessage: %v", err)
				goto exit
			}

			msg := &Message{
				Message: m,
			}
			msg.conn = c
			c.msgChan <- msg
		case common.FrameTypeError:
			log.Infof("FrameTypeError: %v", err)
		default:
			log.Infof("unknown frameType")
		}
	}

exit:
	c.delegate.OnClose(c)
	log.Infof("readLoop exiting")
}

func (c *Conn) Command(cmd *command.Command) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	err := c.conn.SetWriteDeadline(time.Now().Add(common.WriteTimeout))
	if err != nil {
		return err
	}

	if err := cmd.Write(c.conn); err != nil {
		return err
	}

	return nil
}
