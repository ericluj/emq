package emqd

import "net"

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
		state: stateDisconnected,
	}
}

func (lp *LookupPeer) Command(cmd interface{}) ([]byte, error) {
	return nil, nil
}
