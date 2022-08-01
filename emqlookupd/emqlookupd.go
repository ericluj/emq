package emqlookupd

import (
	"fmt"
	"net"
	"sync"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/http_api"
	"github.com/ericluj/emq/internal/protocol"
)

type EMQLookupd struct {
	mtx          sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	tcpServer    *TCPServer
	wg           sync.WaitGroup
	DB           *RegiostrationDB
}

func NewEMQLookupd(opts *Options) (*EMQLookupd, error) {
	var err error
	l := &EMQLookupd{
		opts: opts,
		DB:   NewRegiostrationDB(),
	}

	l.tcpServer = &TCPServer{emqlookupd: l}
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed error: %v", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed error: %v", opts.HTTPAddress, err)
	}
	return l, err
}

func (l *EMQLookupd) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				log.Fatalf("%v", err)
			}
			exitCh <- err
		})
	}

	l.wg.Add(1)
	go func() {
		exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer))
		l.wg.Done()
	}()

	l.wg.Add(1)
	go func() {
		exitFunc(http_api.Serve(l.httpListener, newHTTPServer(l)))
		l.wg.Done()
	}()

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
