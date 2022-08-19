package emqlookupd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

type LookupProtocol struct {
	emqlookupd *EMQLookupd
}

func (l *LookupProtocol) NewClient(conn net.Conn) protocol.Client {
	return NewClient(conn)
}

func (l *LookupProtocol) IOLoop(c protocol.Client) error {
	var (
		err  error
		line []byte
	)

	client := c.(*Client)

	// 接受cmd并执行操作
	for {
		// 设置deadline
		err = client.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
		if err != nil {
			log.Infof("SetReadDeadline error: %v", err)
			return err
		}

		line, err = client.reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				log.Infof("io.EOF")
				err = nil
			} else {
				err = fmt.Errorf("read command error: %v", err)
			}
			break
		}

		// 去掉'\n'（返回的line包含'\n'）
		line = line[:len(line)-1]
		// 去掉'\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// 空格拆分
		params := bytes.Split(line, common.SeparatorBytes)
		log.Infof("PROTOCOL: %s, params: %s", client.conn.RemoteAddr(), params)

		var response []byte
		response, err = l.Exec(client, params)
		if err != nil {
			log.Infof("Exec error: %v", err)
			sendErr := protocol.SendFrameData(client.conn, common.FrameTypeError, []byte(err.Error()))
			if sendErr != nil {
				err = fmt.Errorf("send FrameTypeError error: %v", err)
				break
			}

			// 如果是严重错误，那么断开连接，普通错误则继续执行循环
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			err = protocol.SendFrameData(client.conn, common.FrameTypeMessage, response)
			if err != nil {
				break
			}
		}
	}

	log.Infof("PROTOCOL: %s exiting IOLoop", client.conn.RemoteAddr())

	if client.peerInfo != nil {
		registrations := l.emqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, reg := range registrations {
			if removed, _ := l.emqlookupd.DB.RemoveProducer(reg, client.peerInfo.id); removed {
				log.Infof("RemoveProducer client %s, category:%s key:%s subkey:%s", client.conn.RemoteAddr(), reg.Category, reg.Key, reg.SubKey)
			}
		}
	}

	return err
}

func (l *LookupProtocol) Exec(client *Client, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte(command.PING)):
		return l.PING(client, params)
	case bytes.Equal(params[0], []byte(command.IDENTIFY)):
		return l.IDENTIFY(client, params)
	case bytes.Equal(params[0], []byte(command.REGISTER)):
		return l.REGISTER(client, params)
	case bytes.Equal(params[0], []byte(command.UNREGISTER)):
		return l.UNREGISTER(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, protocol.ErrTypeInvalid, fmt.Sprintf("invalid command %s", params[0]))
}
