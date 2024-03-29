package emqd

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/diskqueue"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/ericluj/emq/internal/util"
)

type Topic struct {
	emqd *EMQD
	mtx  sync.RWMutex
	wg   util.WaitGroup

	name       string
	channelMap map[string]*Channel
	backend    *diskqueue.Diskqueue

	memoryMsgChan     chan *Message
	channelUpdateChan chan int
	startChan         chan int
	exitChan          chan int
	isExiting         int32

	MessageID uint64
}

func (t *Topic) GetName() string {
	return t.name
}

func NewTopic(topicName string, emqd *EMQD) *Topic {
	t := &Topic{
		emqd:              emqd,
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		channelUpdateChan: make(chan int), // topic的channel修改
		memoryMsgChan:     make(chan *Message, emqd.GetOpts().MemQueueSize),
		startChan:         make(chan int),
		exitChan:          make(chan int),
	}

	// 如果设置为0，那么全部走磁盘数据，memoryMsgChan为nil不会再执行到相关逻辑
	if emqd.GetOpts().MemQueueSize == 0 {
		t.memoryMsgChan = nil
	}

	t.backend = diskqueue.New(
		topicName,
		emqd.GetOpts().DataPath,
		emqd.GetOpts().SyncEvery,
		emqd.GetOpts().MaxBytesPerFile,
		int32(emqd.GetOpts().MinMsgSize),
		int32(emqd.GetOpts().MaxMsgSize),
		emqd.GetOpts().SyncTimeout)

	t.wg.Wrap(t.messagePump)

	t.emqd.Notify(t) // 通知lookupd

	return t
}

// topic 执行所有逻辑的地方
func (t *Topic) messagePump() {
	var (
		chans []*Channel
		msg   *Message
	)

	// start后才开始执行数据
	for {
		select {
		case <-t.exitChan:
			goto exit
		// 避免GetChannel时写入channelUpdateChan导致的阻塞
		case <-t.channelUpdateChan:
			continue
		case <-t.startChan:
		}
		break
	}

	chans = t.GetChans()
	for {
		select {
		// channel列表被修改了，重新获取
		case <-t.channelUpdateChan:
			chans = t.GetChans()
			continue
		// 获取到msg，给所有channel发送
		case msg = <-t.memoryMsgChan:

		// 磁盘消息
		case bs := <-t.backend.ReadChan():
			m, err := protocol.DecodeMessage(bs)
			if err != nil {
				log.Errorf("DecodeMessage: %v, topic: %s, msg: %s", err, t.name, msg.ID)
				continue
			}
			msg = &Message{
				Message: m,
			}
		case <-t.exitChan:
			goto exit
		}

		for _, channel := range chans {
			err := channel.PutMessage(msg)
			if err != nil {
				log.Errorf("PutMessage: %v,topic: %s, channel: %s, msg: %s", err, t.name, channel.name, msg.ID)
			}
		}
	}

exit:
	log.Infof("topic: %s, exit messagePump", t.name)
}

func (t *Topic) Start() {
	t.startChan <- 1
}

// 删除
func (t *Topic) Delete() error {
	return t.exit(true)
}

// 关闭，但不删除
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.isExiting) == 1
}

func (t *Topic) exit(deleted bool) error {
	// 避免重复调用
	if !atomic.CompareAndSwapInt32(&t.isExiting, 0, 1) {
		return errors.New("can not exit")
	}

	if deleted {
		log.Infof("topic: %s, deleting", t.name)
		t.emqd.Notify(t) // 通知lookupd
	} else {
		log.Infof("topic: %s, closing", t.name)
	}

	close(t.exitChan)

	t.wg.Wait() // 等待所有数据处理完毕

	if deleted {
		t.mtx.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			_ = channel.Delete()
		}
		t.mtx.Unlock()

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
	log.Infof("topic: %s, new channel: %s", t.name, channelName)

	select {
	case t.channelUpdateChan <- 1: // 通知channel列表变化
	case <-t.exitChan: // topic被关闭的情况
	}

	return c
}

// TODO:
func (t *Topic) GenerateID() protocol.MessageID {
	id := atomic.AddUint64(&t.MessageID, 1)
	b := make([]byte, 8)              // 8bit一个字节，64位8字节
	binary.BigEndian.PutUint64(b, id) // 数字转为大端字节序
	var h protocol.MessageID
	hex.Encode(h[:], b) // hex编码，会将字节长度扩大一倍（为了保证特殊字符的传递？）
	return h
}

func (t *Topic) PutMessage(m *Message) error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.Exiting() {
		return errors.New("can not PutMessage")
	}

	select {
	case t.memoryMsgChan <- m:
	default:
		// 写入磁盘
		data, err := m.Bytes()
		if err != nil {
			log.Errorf("Bytes: %v, topic: %s", err, t.name)
			return err
		}
		err = t.backend.Put(data)
		if err != nil {
			log.Errorf("Put: %v, topic: %s", err, t.name)
			return err
		}
	}

	return nil
}
