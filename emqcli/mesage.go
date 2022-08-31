package emqcli

import (
	"github.com/ericluj/emq/internal/protocol"
)

type Message struct {
	*protocol.Message

	delegate Delegate
	conn     *Conn
}

func (m *Message) Requeue() {
	m.delegate.OnRequeue(m)
}
