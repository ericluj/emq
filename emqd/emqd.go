package emqd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/http_api"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/ericluj/emq/internal/util"
)

type EMQD struct {
	opts atomic.Value
	mtx  sync.RWMutex
	wg   util.WaitGroup

	idGenerator int64
	isLoading   int32

	topicMap    map[string]*Topic
	lookupPeers atomic.Value

	exitChan   chan int         // 程序退出信号
	notifyChan chan interface{} // 通知信号

	tcpListener  net.Listener
	httpListener net.Listener
	tcpServer    *TCPServer
}

func NewEMQD(opts *Options) (*EMQD, error) {
	e := &EMQD{
		topicMap:   make(map[string]*Topic),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
	}

	var err error
	e.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen %s error: %v", opts.TCPAddress, err)
	}
	e.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen %s error: %v", opts.HTTPAddress, err)
	}
	e.tcpServer = &TCPServer{emqd: e}

	e.opts.Store(opts)
	return e, nil
}

func (e *EMQD) Main() error {
	var once sync.Once
	exitCh := make(chan error)
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				log.Infof("exitFunc error: %v", err)
			}
			exitCh <- err
		})
	}

	// tcp server
	e.wg.Wrap(func() {
		exitFunc(protocol.TCPServer(e.tcpListener, e.tcpServer))
	})

	// http server
	e.wg.Wrap(func() {
		exitFunc(http_api.Serve(e.httpListener, newHTTPServer()))
	})

	// lookupLoop
	e.wg.Wrap(e.lookupLoop)

	// Main方法被阻塞，直到server有错误的时候往下返回err
	err := <-exitCh
	return err
}

func (e *EMQD) Exit() {
	if e.tcpListener != nil {
		e.tcpListener.Close()
	}

	if e.tcpServer != nil {
		e.tcpServer.Close()
	}

	if e.httpListener != nil {
		e.httpListener.Close()
	}

	e.mtx.Lock()
	// 关闭所有topic
	for _, topic := range e.topicMap {
		topic.Close()
	}
	e.mtx.Unlock()

	// 通知所有goruntine关闭
	close(e.exitChan)

	// 等待goruntine都处理完毕
	e.wg.Wait()

	log.Infof("EMQD exit")
}

func (e *EMQD) GetOpts() *Options {
	return e.opts.Load().(*Options)
}

// EMQD中的数据发生了变化
func (e *EMQD) Notify(v interface{}) {
	e.wg.Wrap(func() {
		select {
		case <-e.exitChan: // 避免阻止exit（可能的死锁问题）
		case e.notifyChan <- v:
			// 加载中不需要持久化
			if atomic.LoadInt32(&e.isLoading) == 1 {
				return
			}

			e.mtx.Lock()
			err := e.PersistMetadata()
			if err != nil {
				log.Infof("PersistMetadata error: %v", err)
			}
			e.mtx.Unlock()
		}
	})
}

func (e *EMQD) GetTopic(topicName string) *Topic {
	// 可以不要这个读锁，直接写锁然后去处理，这么做为了提升性能
	e.mtx.RLock() // 加读锁，防止被写入
	t, ok := e.topicMap[topicName]
	e.mtx.RUnlock() // 读完了，释放读锁
	if ok {
		return t
	}

	// 没有读取到，那么去创建
	e.mtx.Lock()                  // 加写锁，防止读写
	t, ok = e.topicMap[topicName] // 为什么要加锁后再读一次？因为可能会RUnlock到Lock这一段时间内被写入数据了
	if ok {
		e.mtx.Unlock()
		return t
	}

	t = NewTopic(topicName, e)
	e.topicMap[topicName] = t
	e.mtx.Unlock()
	log.Infof("topic: %s, created", t.name)

	// 如果是在加载数据中，那么不需要进行后面的初始化操作
	if atomic.LoadInt32(&e.isLoading) == 1 {
		return t
	}

	// TODO: lookupd相关操作

	t.Start()
	return t
}

func (e *EMQD) LoadMetadata() error {
	atomic.StoreInt32(&e.isLoading, 1) // 数据加载中
	defer atomic.StoreInt32(&e.isLoading, 0)

	fileName := metadataFile(e.GetOpts())

	data, err := util.ReadFile(fileName)
	if err != nil {
		return err
	}

	// 没有读取到文件内容，不往下执行了
	if data == nil {
		return nil
	}

	var m Metadata
	err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	for _, t := range m.Topics {
		topic := e.GetTopic(t.Name)
		for _, c := range t.Channels {
			topic.GetChannel(c.Name)
		}
		topic.Start()
	}

	return nil
}

func (e *EMQD) PersistMetadata() error {
	fileName := metadataFile(e.GetOpts())
	log.Infof("PersistMetadata to: %s", fileName)

	// 获取数据
	data, err := json.Marshal(e.GetMetadata())
	if err != nil {
		return err
	}

	// 写到临时文件
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	err = util.WriteSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}

	// 临时文件改名
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (e *EMQD) GetMetadata() *Metadata {
	m := &Metadata{
		Topics: make([]TopicMetadata, 0),
	}

	for _, topic := range e.topicMap {
		topicData := TopicMetadata{
			Name: topic.name,
		}

		topic.mtx.Lock()
		for _, channel := range topic.channelMap {
			topicData.Channels = append(topicData.Channels, ChannelMetadata{
				Name: channel.name,
			})
		}
		topic.mtx.Unlock()
		m.Topics = append(m.Topics, topicData)
	}

	return m
}
