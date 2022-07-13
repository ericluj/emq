package common

const (
	FrameTypeResponse int32 = 0
	FrameTypeError    int32 = 1
	FrameTypeMessage  int32 = 2
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

var (
	HeartbeatBytes = []byte("_heartbeat_")
	SeparatorBytes = []byte(" ")
	OKBytes        = []byte("OK")
)
