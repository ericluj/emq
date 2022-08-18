package emqlookupd

type PeerInfo struct {
	id         string
	lastUpdate int64

	TCPAddress       string `json:"tcp_address"`
	HTTPAddress      string `json:"http_address"`
	BroadcastAddress string `json:"broadcast_address"`
}
