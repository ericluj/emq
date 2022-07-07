package emqd

import "sync"

type Channel struct {
	name      string
	topicName string
	emqd      *EMQD

	memoryMsgChan chan *Message
	clients       map[int64]*Client
	sync.RWMutex
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

func (c *Channel) PutMessage(msg *Message) error {
	select {
	case c.memoryMsgChan <- msg:
	default:
		// TODO:落磁盘
	}
	return nil
}

func (c *Channel) AddClient(client *Client) error {
	c.RLock()
	_, ok := c.clients[client.ID]
	c.RUnlock()
	if ok {
		return nil
	}

	c.Lock()
	c.clients[client.ID] = client
	c.Unlock()
	return nil
}
