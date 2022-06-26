package emqd

import (
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/ericluj/elog"
)

type EMQD struct {
	startTime time.Time

	topicMap map[string]*Topic

	tcpListener  net.Listener
	httpListener net.Listener

	exitChan  chan int         // 程序退出信号
	waitGroup WaitGroupWrapper // 平滑退出
}

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
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
	e.waitGroup.Wrap(func() {
		exitFunc(TCPServer(e.tcpListener))
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
