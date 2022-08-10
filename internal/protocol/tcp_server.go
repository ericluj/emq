package protocol

import (
	"net"
	"runtime"
	"strings"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/util"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler) error {
	log.Infof("TCP: listening on %s", listener.Addr())

	var wg util.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				log.Infof("network timeout error: %v", err)
				// 如果是超时等临时错误，先暂停当前goruntine交出调度，时间片轮转到后再恢复后续操作
				runtime.Gosched()
				continue
			}

			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Infof("network error: %v", err)
				return err
			}

			break
		}

		wg.Wrap(func() {
			handler.Handle(clientConn)
		})
	}

	wg.Wait()

	log.Infof("TCP: closing %s", listener.Addr())

	return nil
}
