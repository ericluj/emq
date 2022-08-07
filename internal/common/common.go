package common

import (
	"time"
)

const (
	FrameTypeResponse int32 = 0
	FrameTypeError    int32 = 1
	FrameTypeMessage  int32 = 2
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
	DialTimeout      = time.Second
	ReadTimeout      = time.Second * 60
	WriteTimeout     = time.Second
	HeartbeatTimeout = time.Second * 30
)

var (
	ProtoMagic        = "  V1"
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
