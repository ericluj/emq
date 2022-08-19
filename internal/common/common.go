package common

import (
	"time"
)

const (
	FrameTypeError int32 = iota
	FrameTypeMessage
)

const (
	MsgIDLength = 16
)

var (
	HeartbeatBytes   = []byte("_heartbeat_")
	SeparatorBytes   = []byte(" ")
	OKBytes          = []byte("OK")
	BadProtocolBytes = []byte("E_BAD_PROTOCOL")
	NewLineBytes     = []byte("\n")
)

var (
	DialTimeout      = time.Second * 5
	ReadTimeout      = time.Second * 30
	WriteTimeout     = time.Second * 5
	HeartbeatTimeout = time.Second * 15
)

var (
	ProtoMagic        = "  V1"
	ProtoMagicLen     = 4 // ProtoMagic的长度
	DefaultBufferSize = 16 * 1024
)

// client
const (
	ClientInit = iota
	ClientSubscribed
)

// lookup_peer
const (
	PeerInit = iota
	PeerConnected
)

// consumer
const (
	ConsumerInit = iota
	ConsumerConnected
)
