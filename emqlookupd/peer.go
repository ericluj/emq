package emqlookupd

type PeerInfo struct {
	id         string
	lastUpdate int64

	RemoteAddress    string `json:"remote_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	BroadcastAddress string `json:"broadcast_address"`
}
