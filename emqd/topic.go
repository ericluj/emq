package emqd

import "emq/internal/util"

type Topic struct {
	name string
	emqd *EMQD

	channelMap    map[string]*Channel
	memoryMsgChan chan *Message
	startChan     chan int
	exitChan      chan int

	waitGroup util.WaitGroupWrapper
}

func NewTopic(topicName string, emqd *EMQD) *Topic {
	t := &Topic{
		name:          topicName,
		emqd:          emqd,
		channelMap:    make(map[string]*Channel),
		memoryMsgChan: make(chan *Message, emqd.getOpts().MemQueueSize),
		startChan:     make(chan int, 1), // 之所以给1的缓冲，是防止start方法被阻塞，如果不给1那么必须得被
		exitChan:      make(chan int),
	}

	t.waitGroup.Wrap(t.messagePump)

	return t
}

func (t *Topic) messagePump() {

}

func (t *Topic) Start() {
	t.startChan <- 1 // TODO: 是否要select来防止多处start阻塞
}
