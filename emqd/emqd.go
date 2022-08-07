package emqd

import (
	"fmt"
	"net"
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

	exitChan  chan int // 程序退出信号
	isLoading int32

	lookupPeers      atomic.Value
	notifyChan       chan interface{} // 通知lookupd信号
	topicMap         map[string]*Topic
	clientIDSequence int64

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
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	e.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}
	e.tcpServer = &TCPServer{emqd: e}

	e.opts.Store(opts)
	return e, nil
}

func (e *EMQD) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				log.Infof("error: %v", err)
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

	log.Infof("EMQ: bye")
}

func (e *EMQD) Notify(v interface{}) {
	e.wg.Wrap(func() {
		select {
		// 避免阻止exit
		case <-e.exitChan:
		case e.notifyChan <- v:
			if atomic.LoadInt32(&e.isLoading) == 1 {
				return
			}

			// TODO: 数据持久化
		}
	})
}

func (e *EMQD) getOpts() *Options {
	return e.opts.Load().(*Options)
}

func (e *EMQD) LoadMetadata() error {
	atomic.StoreInt32(&e.isLoading, 1)       // 数据加载中
	defer atomic.StoreInt32(&e.isLoading, 0) // 加载完毕变成初始状态

	return nil
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
	log.Infof("TOPIC(%s): created", t.name)

	// 如果是在加载数据中，那么不需要进行后面的初始化操作
	if atomic.LoadInt32(&e.isLoading) == 1 {
		return t
	}

	t.Start()
	return t
}
