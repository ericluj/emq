package emqd

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/util"
)

type Topic struct {
	emqd *EMQD
	name string
	mtx  sync.RWMutex
	wg   util.WaitGroup

	startChan chan int
	exitChan  chan int
	isExiting int32

	channelUpdateChan chan int
	channelMap        map[string]*Channel
	memoryMsgChan     chan *Message

	MessageID uint64
}

func (t *Topic) GetName() string {
	return t.name
}

func NewTopic(topicName string, emqd *EMQD) *Topic {
	t := &Topic{
		name:              topicName,
		emqd:              emqd,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, emqd.getOpts().MemQueueSize),
		startChan:         make(chan int),
		exitChan:          make(chan int), // topic关闭
		channelUpdateChan: make(chan int), // topic的channel修改
	}

	t.wg.Wrap(t.messagePump)

	t.emqd.Notify(t) // 通知lookupd

	return t
}

// topic 执行所有逻辑的地方
func (t *Topic) messagePump() {
	var (
		chans         []*Channel
		memoryMsgChan chan *Message
	)

	// 阻塞等待start
	select {
	case <-t.exitChan:
		goto exit
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
			goto exit
		}
	}

exit:
	log.Infof("TOPIC(%s): closing ... messagePump", t.name)
}

func (t *Topic) Start() {
	t.startChan <- 1
}

func (t *Topic) Delete() error {
	return t.exit(true)
}

func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.isExiting) == 1
}

func (t *Topic) exit(deleted bool) error {
	// 避免重复调用
	if !atomic.CompareAndSwapInt32(&t.isExiting, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		log.Infof("TOPIC(%s): deleting", t.name)
		t.emqd.Notify(t) // 通知lookupd
	} else {
		log.Infof("TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	t.wg.Wait() // 等待所有数据处理完毕

	if deleted {
		t.mtx.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Close()
		}
		t.mtx.Unlock()

		// TODO: 删除files
		return nil
	}

	t.mtx.RLock()
	for _, channel := range t.channelMap {
		channel.Close()
	}
	t.mtx.RUnlock()

	return nil
}

func (t *Topic) GetChans() []*Channel {
	chans := make([]*Channel, 0)
	t.mtx.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.mtx.RUnlock()
	return chans
}

func (t *Topic) GetChannel(channelName string) *Channel {
	t.mtx.RLock()
	c, ok := t.channelMap[channelName]
	t.mtx.RUnlock()
	if ok {
		return c
	}

	t.mtx.Lock()
	c, ok = t.channelMap[channelName]
	if ok {
		t.mtx.Unlock()
		return c
	}

	c = NewChannel(t.name, channelName, t.emqd)
	t.channelMap[channelName] = c
	t.mtx.Unlock()
	log.Infof("TOPIC(%s): new channel(%s)", t.name, channelName)

	select {
	case t.channelUpdateChan <- 1: // 通知channel列表变化
	case <-t.exitChan: // topic被关闭的情况
	}

	return c
}

func (t *Topic) GenerateID() MessageID {
	id := atomic.AddUint64(&t.MessageID, 1)
	b := make([]byte, 8)              // 8bit一个字节，64位8字节
	binary.BigEndian.PutUint64(b, id) // 数字转为大端字节序
	var h MessageID
	hex.Encode(h[:], b) // hex编码，会将字节长度扩大一倍（为了保证特殊字符的传递？）
	return h
}

func (t *Topic) PutMessage(m *Message) error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.Exiting() {
		return errors.New("exiting")
	}

	select {
	case t.memoryMsgChan <- m:
	default:
		// TODO: 如果写满了，放到磁盘
	}

	return nil
}
