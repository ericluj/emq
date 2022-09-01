package emqcli

import (
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
)

type Producer struct {
	id int64

	conn *Conn
}

func NewProducer(addr string) (*Producer, error) {
	p := &Producer{
		id: atomic.AddInt64(&instCount, 1),
	}
	p.conn = NewConn(addr, nil, p)
	err := p.conn.Connect()
	if err != nil {
		log.Infof("Connect error: %v", err)
		return nil, err
	}

	return p, nil
}

func (p *Producer) Publish(topic string, body string) error {
	cmd := command.PublishCmd(topic, []byte(body))
	err := p.conn.Command(cmd)
	return err
}

func (p *Producer) OnClose(conn *Conn) {

}

func (p *Producer) OnRequeue(msg *Message) {

}
