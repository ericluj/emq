package emqlookupd

import (
	"net"
	"sync"
)

type TCPServer struct {
	emqlookupd *EMQLookupd
	conns      sync.Map
}

func (s *TCPServer) Handle(conn net.Conn) {

}

func (s *TCPServer) Close() {

}
