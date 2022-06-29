package emqd

type Channel struct {
	name      string
	topicName string
	emqd      *EMQD

	memoryMsgChan chan *Message
}

func NewChannel(topicName, channelName string, emqd *EMQD) *Channel {
	c := &Channel{
		topicName:     topicName,
		name:          channelName,
		emqd:          emqd,
		memoryMsgChan: make(chan *Message, emqd.getOpts().MemQueueSize),
	}

	return c
}

func (c Channel) PutMessage(msg *Message) error {
	return nil
}
