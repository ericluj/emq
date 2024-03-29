package emqd

import (
	"bufio"
	"errors"
	"net"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

type LookupPeer struct {
	addr   string
	conn   net.Conn
	reader *bufio.Reader
	state  int32
	// info  PeerInfo
}

type PeerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	BroadcastAddress string `json:"broadcast_address"`
}

func NewLookupPeer(addr string) *LookupPeer {
	return &LookupPeer{
		addr:  addr,
		state: common.PeerInit,
	}
}

func (lp *LookupPeer) Connect(e *EMQD) error {
	log.Infof("lookup connect to %s", lp.addr)

	initialState := lp.state

	// tcp连接
	conn, err := net.DialTimeout("tcp", lp.addr, common.DialTimeout)
	if err != nil {
		return err
	}
	lp.conn = conn
	lp.reader = bufio.NewReaderSize(conn, common.DefaultBufferSize)
	lp.state = common.PeerConnected

	// 如果不是第一次连接，不需要下面的逻辑
	if initialState != common.PeerInit {
		return nil
	}

	// 协议版本
	_, err = lp.Write([]byte(common.ProtoMagic))
	if err != nil {
		lp.Close()
		return err
	}

	// identify
	identifyData := map[string]interface{}{
		"tcp_address":  e.GetOpts().TCPAddress,
		"http_address": e.GetOpts().HTTPAddress,
	}
	cmd, err := command.IDENTIFYCmd(identifyData)
	if err != nil {
		lp.Close()
		return err
	}
	resp, err := lp.Command(cmd)
	if err != nil {
		return err
	}
	log.Debugf("IDENTIFYCmd resp: %s", resp)
	// TODO: 返回数据待处理

	// 注册到lookupd
	var cmds []*command.Command
	e.mtx.RLock()
	for _, topic := range e.topicMap {
		topic.mtx.RLock()
		if len(topic.channelMap) == 0 {
			cmds = append(cmds, command.RegisterCmd(topic.name, ""))
		} else {
			for _, channel := range topic.channelMap {
				cmds = append(cmds, command.RegisterCmd(channel.topicName, channel.name))
			}
		}
		topic.mtx.RUnlock()
	}
	e.mtx.RUnlock()

	for _, cmd := range cmds {
		log.Infof("lookup: %s, cmd: %s", lp.addr, cmd)
		_, err := lp.Command(cmd)
		if err != nil {
			log.Errorf("Command: %v, lookup: %s, cmd: %v,", err, lp.addr, cmd)
			return err
		}
	}

	return nil
}

func (lp *LookupPeer) Write(data []byte) (int, error) {
	err := lp.conn.SetWriteDeadline(time.Now().Add(common.WriteTimeout))
	if err != nil {
		return 0, err
	}
	return lp.conn.Write(data)
}

func (lp *LookupPeer) Read(data []byte) (int, error) {
	err := lp.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
	if err != nil {
		return 0, err
	}
	return lp.conn.Read(data)
}

func (lp *LookupPeer) Close() error {
	lp.state = common.PeerInit
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

func (lp *LookupPeer) Command(cmd *command.Command) ([]byte, error) {

	err := lp.conn.SetWriteDeadline(time.Now().Add(common.WriteTimeout))
	if err != nil {
		return nil, err
	}

	if err := cmd.Write(lp.conn); err != nil {
		lp.Close()
		return nil, err
	}

	frameType, body, err := protocol.ReadFrameData(lp.reader)
	if err != nil {
		lp.Close()
		return nil, err
	}
	if frameType == common.FrameTypeError {
		return nil, errors.New(string(body))
	}

	return body, nil
}
