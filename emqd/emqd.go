package emqd

import (
	"fmt"
	"net"
	"time"

	log "github.com/ericluj/elog"
)

type EMQD struct {
	startTime time.Time

	topicMap map[string]*Topic

	exitChan chan int

	tcpListener  net.Listener
	httpListener net.Listener
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
	log.Infof("main...")
	return nil
}

func (e *EMQD) Exit() error {
	log.Infof("exit...")
	return nil
}
