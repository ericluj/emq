package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	log "github.com/ericluj/elog"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler) error {
	log.Infof("TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				log.Infof("temporary Accept() failure - %s", err)
				// 如果是超时等临时错误，先暂停当前goruntine交出调度，时间片轮转到后再恢复后续操作
				runtime.Gosched()
				continue
			}

			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}

			break
		}

		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	wg.Wait()

	log.Infof("TCP: closing %s", listener.Addr())

	return nil
}
