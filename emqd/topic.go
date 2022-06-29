package emqd

import (
	"emq/internal/util"
	"sync"

	log "github.com/ericluj/elog"
)

type Topic struct {
	name string
	emqd *EMQD
	sync.RWMutex
	channelMap        map[string]*Channel
	memoryMsgChan     chan *Message
	startChan         chan int
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
}

func NewTopic(topicName string, emqd *EMQD) *Topic {
	t := &Topic{
		name:              topicName,
		emqd:              emqd,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, emqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1), // 之所以给1的缓冲，是防止start方法被阻塞，如果不给1那么必须得被读取才能写
		exitChan:          make(chan int),    // topic关闭
		channelUpdateChan: make(chan int),    // topic的channel修改
	}

	t.waitGroup.Wrap(t.messagePump)

	return t
}

func (t *Topic) messagePump() {
	var (
		chans         []*Channel
		memoryMsgChan chan *Message
	)

	// 等待start
	select {
	case <-t.startChan:
	}

	chans = t.GetChans()
	memoryMsgChan = t.memoryMsgChan

	for {
		select {
		// channel列表被修改了，重新获取
		case <-t.channelUpdateChan:
			chans = t.GetChans()
		// 获取到msg，给所有channel发送
		case msg := <-memoryMsgChan:
			for _, channel := range chans {
				err := channel.PutMessage(msg)
				if err != nil {
					log.Infof("TOPIC(%s) error: failed to put msg(%s) to channel(%s) - %s", t.name, msg.ID, channel.name, err)
				}
			}
		case <-t.exitChan:
			log.Infof("TOPIC(%s): closing ... messagePump", t.name)
			return
		}
	}
}

func (t *Topic) GetChans() []*Channel {
	chans := make([]*Channel, 0)
	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	return chans
}

func (t *Topic) Start() {
	t.startChan <- 1 // TODO: 是否要select来防止多处start阻塞
}

func (t *Topic) GetChannel(channelName string) *Channel {
	t.RLock()
	c, ok := t.channelMap[channelName]
	t.Unlock()
	if ok {
		return c
	}

	t.Lock()
	c, ok = t.channelMap[channelName]
	if ok {
		t.Unlock()
		return c
	}

	c = NewChannel(t.name, channelName, t.emqd)
	t.channelMap[channelName] = c
	log.Infof("TOPIC(%s): new channel(%s)", t.name, channelName)

	select {
	case t.channelUpdateChan <- 1: // 通知channel列表变化
	case <-t.exitChan:
	}

	return c
}
