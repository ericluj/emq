package emqcli

type ConnDelegate interface {
	OnResponse(*Conn, []byte)

	OnError(*Conn, []byte)

	OnMessage(*Conn, *Message)

	OnIOError(*Conn, error)

	OnHeartbeat(*Conn)
}

type consumerConnDelegate struct {
	r *Consumer
}

func (d *consumerConnDelegate) OnResponse(c *Conn, data []byte) { d.r.onConnResponse(c, data) }
func (d *consumerConnDelegate) OnError(c *Conn, data []byte)    { d.r.onConnError(c, data) }
func (d *consumerConnDelegate) OnMessage(c *Conn, m *Message)   { d.r.onConnMessage(c, m) }
func (d *consumerConnDelegate) OnIOError(c *Conn, err error)    { d.r.onConnIOError(c, err) }
func (d *consumerConnDelegate) OnHeartbeat(c *Conn)             { d.r.onConnHeartbeat(c) }

type producerConnDelegate struct {
	w *Producer
}

func (d *producerConnDelegate) OnResponse(c *Conn, data []byte) {}
func (d *producerConnDelegate) OnError(c *Conn, data []byte)    {}
func (d *producerConnDelegate) OnMessage(c *Conn, m *Message)   {}
func (d *producerConnDelegate) OnIOError(c *Conn, err error)    {}
func (d *producerConnDelegate) OnHeartbeat(c *Conn)             {}
