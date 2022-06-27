package emqd

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/ericluj/elog"
)

type TCPServer struct {
	emqd  *EMQD
	conns sync.Map
}

func (t *TCPServer) Init(listener net.Listener) error {
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
			t.Handle(clientConn)
			wg.Done()
		}()
	}

	wg.Wait()

	log.Infof("TCP: closing %s", listener.Addr())

	return nil
}

func (t *TCPServer) Handle(conn net.Conn) {
	log.Infof("TCP: new client(%s)", conn.RemoteAddr())
	client := t.NewClient(conn)
	t.conns.Store(conn.RemoteAddr(), client)

	// client循环处理工作
	err := client.IOLoop()
	if err != nil {
		log.Infof("client(%s) error: %v", conn.RemoteAddr(), err)
	}

	t.conns.Delete((conn.RemoteAddr()))
	conn.Close()
}

func (t *TCPServer) NewClient(conn net.Conn) *Client {
	clientID := atomic.AddInt64(&t.emqd.clientIDSequence, 1)
	c := &Client{
		ID: clientID,
	}
	return c
}
