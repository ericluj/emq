package emqd

import (
	"errors"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
)

type Channel struct {
	emqd *EMQD
	mtx  sync.RWMutex

	topicName string
	name      string
	clients   map[int64]*Client

	memoryMsgChan chan *Message
	isExiting     int32

	inFlightMtx      sync.Mutex
	inFlightMessages map[MessageID]*Message
}

func (c *Channel) GetName() string {
	return c.name
}

func (c *Channel) GetMemoryMsgChan() chan *Message {
	return c.memoryMsgChan
}

func NewChannel(topicName, channelName string, emqd *EMQD) *Channel {
	c := &Channel{
		emqd:             emqd,
		topicName:        topicName,
		name:             channelName,
		clients:          make(map[int64]*Client),
		memoryMsgChan:    make(chan *Message, emqd.GetOpts().MemQueueSize),
		inFlightMessages: make(map[MessageID]*Message),
	}

	c.emqd.Notify(c) // 通知lookupd

	return c
}

func (c *Channel) Delete() error {
	return c.exit(true)
}

func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.isExiting) == 1
}

func (c *Channel) exit(deleted bool) error {
	// 避免重复调用
	if !atomic.CompareAndSwapInt32(&c.isExiting, 0, 1) {
		return errors.New("can not exit")
	}

	if deleted {
		log.Infof("channel: %s, deleting", c.name)
		c.emqd.Notify(c) // 通知lookupd
	} else {
		log.Infof("channel: %s, closing", c.name)
	}

	c.mtx.RLock()
	for _, client := range c.clients {
		client.conn.Close()
	}
	c.mtx.RUnlock()

	if deleted {
		c.Empty()
	}

	// TODO: 待处理
	return nil
}

// 清空数据
func (c *Channel) Empty() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for {
		select {
		case <-c.memoryMsgChan:
		default:
			return
		}
	}
}

func (c *Channel) PutMessage(msg *Message) error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	if c.Exiting() {
		return errors.New("can not PutMessage")
	}

	select {
	case c.memoryMsgChan <- msg:
	default:
		break
	}

	// TODO: 写入磁盘

	return nil
}

func (c *Channel) AddClient(client *Client) error {
	if c.Exiting() {
		return errors.New("can not AddClient")
	}

	c.mtx.RLock()
	_, ok := c.clients[client.ID]
	c.mtx.RUnlock()
	if ok {
		return nil
	}

	c.mtx.Lock()
	_, ok = c.clients[client.ID]
	if ok {
		c.mtx.Unlock()
		return nil
	}
	c.clients[client.ID] = client
	c.mtx.Unlock()
	return nil
}

func (c *Channel) RemoveClient(clientID int64) {
	if c.Exiting() {
		return
	}

	c.mtx.RLock()
	_, ok := c.clients[clientID]
	c.mtx.RUnlock()
	if !ok {
		return
	}

	c.mtx.Lock()
	delete(c.clients, clientID)
	c.mtx.Unlock()
}

func (c *Channel) RequeueMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}

	// TODO: exitMtx是否需要
	if c.Exiting() {
		return errors.New("exiting")
	}
	return c.PutMessage(msg)
}

func (c *Channel) pushInFlightMessage(msg *Message) {
	c.inFlightMtx.Lock()
	defer c.inFlightMtx.Unlock()

	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		return
	}

	c.inFlightMessages[msg.ID] = msg
}

func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMtx.Lock()
	defer c.inFlightMtx.Unlock()

	msg, ok := c.inFlightMessages[id]
	if !ok {
		return nil, errors.New("ID not in flight")
	}

	if msg.ClientID != clientID {
		return nil, errors.New("client does not own message")
	}

	delete(c.inFlightMessages, id)
	return msg, nil
}

func (c *Channel) StartInFlight(msg *Message, clientID int64) {
	msg.ClientID = clientID
	c.pushInFlightMessage(msg)
}
