package emqcli

import (
	"sync/atomic"

	log "github.com/ericluj/elog"
)

type Producer struct {
	id   int64
	addr string
	conn *Conn
}

func NewProducer(addr string) (*Producer, error) {
	p := &Producer{
		id:   atomic.AddInt64(&instCount, 1),
		addr: addr,
		conn: NewConn(addr),
	}

	err := p.conn.Connect()
	if err != nil {
		log.Infof("Connect error: %v", err)
		return nil, err
	}

	return p, nil
}

func (p *Producer) Publish(topic string, body string) error {
	return p.conn.WriteCommand(Publish(topic, []byte(body)))
}
