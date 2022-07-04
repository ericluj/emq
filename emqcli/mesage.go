package emqcli

const MsgIDLength = 16

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	EMQDAddress string
}

func (m *Message) Finish() {

}
