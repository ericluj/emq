package emqd

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"emq/internal/util"

	log "github.com/ericluj/elog"
)

type EMQD struct {
	opts atomic.Value
	sync.RWMutex
	startTime time.Time

	topicMap map[string]*Topic

	clientIDSequence int64

	tcpListener  net.Listener
	httpListener net.Listener
	tcpServer    *TCPServer

	exitChan  chan int              // 程序退出信号
	waitGroup util.WaitGroupWrapper // 平滑退出
}

func NewEMQD(opts *Options) (*EMQD, error) {
	e := &EMQD{
		startTime: time.Now(),
		topicMap:  make(map[string]*Topic),
		exitChan:  make(chan int),
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

func (e *EMQD) getOpts() *Options {
	return e.opts.Load().(*Options)
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
	e.waitGroup.Wrap(func() {
		exitFunc(e.tcpServer.Init(e.tcpListener))
	})

	// http server
	e.waitGroup.Wrap(func() {
		exitFunc(HTTPServer(e.httpListener))
	})

	err := <-exitCh
	return err
}

func (e *EMQD) Exit() {
	if e.tcpListener != nil {
		e.tcpListener.Close()
	}

	if e.httpListener != nil {
		e.httpListener.Close()
	}

	// 等待goruntine都处理完毕
	close(e.exitChan)
	e.waitGroup.Wait()

	log.Infof("EMQ: bye")
}

func (e *EMQD) GetTopic(topicName string) *Topic {
	e.RLock() // 加读锁，防止被写入
	t, ok := e.topicMap[topicName]
	e.RUnlock() // 读完了，释放读锁
	if ok {
		return t
	}

	// 没有读取到，那么去创建
	e.Lock()                      // 加写锁，防止读写
	t, ok = e.topicMap[topicName] // TODO: 这里为什么要再读判断一次呢，设么时候可能会出现这种第二次读到的情况？
	if ok {
		e.Unlock()
		return t
	}

	t = NewTopic(topicName, e)
	e.topicMap[topicName] = t
	e.Unlock()
	log.Infof("TOPIC(%s): created", t.name)

	t.Start()
	return t
}
