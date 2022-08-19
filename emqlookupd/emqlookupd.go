package emqlookupd

import (
	"fmt"
	"net"
	"sync"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/http_api"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/ericluj/emq/internal/util"
)

type EMQLookupd struct {
	opts *Options
	wg   util.WaitGroup

	DB *RegiostrationDB

	tcpListener  net.Listener
	httpListener net.Listener
	tcpServer    *TCPServer
}

func NewEMQLookupd(opts *Options) (*EMQLookupd, error) {
	l := &EMQLookupd{
		opts: opts,
		DB:   NewRegiostrationDB(),
	}

	var err error
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen %s error: %v", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen %s error: %v", opts.HTTPAddress, err)
	}
	l.tcpServer = &TCPServer{emqlookupd: l}
	return l, err
}

func (l *EMQLookupd) GetOpts() *Options {
	return l.opts
}

func (l *EMQLookupd) Main() error {
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
	l.wg.Wrap(func() {
		exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer))
	})

	// http server
	l.wg.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, newHTTPServer(l)))
	})

	// Main方法被阻塞，直到server有错误的时候往下返回err
	err := <-exitCh
	return err
}

func (l *EMQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.tcpServer != nil {
		l.tcpServer.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}

	l.wg.Wait()
}
