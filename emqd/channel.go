package emqd

type Channel struct {
	name      string
	topicName string
	emqd      *EMQD

	memoryMsgChan chan *Message
}
