package emqd

import (
	"fmt"
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
	Info  PeerInfo
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

func (lp *LookupPeer) Connect() error {
	log.Infof("LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, common.DialTimeout)
	if err != nil {
		return err
	}
	lp.conn = conn
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
	initalState := lp.state

	// 没有连接的话进行connect
	if initalState != common.PeerConnected {
		if err := lp.Connect(); err != nil {
			return nil, err
		}
		lp.state = common.PeerConnected
		if _, err := lp.Write([]byte(common.ProtoMagic)); err != nil {
			lp.conn.Close()
			return nil, err
		}

		// TODO: connect callback

		if lp.state != common.PeerConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}

	// nil只需要进行connect
	if cmd == nil {
		return nil, nil
	}

	if err := cmd.Write(lp); err != nil {
		lp.Close()
		return nil, err
	}

	resp, err := protocol.ReadData(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}

	return resp, nil
}
