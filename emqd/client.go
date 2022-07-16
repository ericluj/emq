package emqd

import (
	"bufio"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type Client struct {
	ID   int64
	emqd *EMQD
	net.Conn
	tlsConn *tls.Conn
	TLS     int32

	Reader *bufio.Reader
	Writer *bufio.Writer

	Channel      *Channel      // client订阅的channel
	SubEventChan chan *Channel // 事件，说明client有订阅
	State        int32

	writeLock sync.RWMutex

	ExitChan chan int
	lenBuf   [4]byte
	lenSlice []byte
}

type identifyData struct {
}

func (client *Client) UpgradeTLS() error {
	client.writeLock.Lock()
	defer client.writeLock.Unlock()

	tlsConn := tls.Server(client.Conn, client.emqd.tlsConf)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	client.tlsConn = tlsConn

	client.Reader = bufio.NewReaderSize(client.tlsConn, defaultBufferSize)
	client.Writer = bufio.NewWriterSize(client.tlsConn, defaultBufferSize)

	atomic.StoreInt32(&client.TLS, 1)

	return nil
}
