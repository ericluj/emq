package emqd

import (
	"errors"
	"sync"
	"sync/atomic"
)

type Channel struct {
	name      string
	topicName string
	emqd      *EMQD

	exitFlag      int32
	memoryMsgChan chan *Message
	clients       map[int64]*Client
	mtx           sync.RWMutex
}

func NewChannel(topicName, channelName string, emqd *EMQD) *Channel {
	c := &Channel{
		topicName:     topicName,
		name:          channelName,
		emqd:          emqd,
		memoryMsgChan: make(chan *Message, emqd.getOpts().MemQueueSize),
		clients:       make(map[int64]*Client),
	}

	return c
}

func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

func (c *Channel) PutMessage(msg *Message) error {
	select {
	case c.memoryMsgChan <- msg:
	default:
		// TODO:落磁盘
	}
	return nil
}

func (c *Channel) AddClient(client *Client) error {
	if c.Exiting() {
		return errors.New("exiting")
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

	// TODO: 额外处理
}

func (c *Channel) Delete() error {
	return c.exit(true)
}

func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {

	// TODO: 处理
	return nil
}
