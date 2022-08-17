package emqd

import (
	"net"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

type LookupPeer struct {
	addr  string
	conn  net.Conn
	state int32
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
	identifyData := map[string]interface{}{}
	cmd, err := command.IDENTIFYCmd(identifyData)
	if err != nil {
		lp.Close()
		return err
	}
	_, err = lp.Command(cmd)
	if err != nil {
		return err
	}
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
	e.mtx.RLock()

	for _, cmd := range cmds {
		log.Infof("lookup: %s, cmd: %v", lp.addr, cmd)
		_, err := lp.Command(cmd)
		if err != nil {
			log.Infof("lookup: %s, cmd: %v, error: %v", lp.addr, cmd, err)
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
	if err := cmd.Write(lp.conn); err != nil {
		lp.Close()
		return nil, err
	}

	resp, err := protocol.ReadData(lp.conn)
	if err != nil {
		lp.Close()
		return nil, err
	}

	return resp, nil
}
