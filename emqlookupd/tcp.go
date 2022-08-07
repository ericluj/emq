package emqlookupd

import (
	"io"
	"net"
	"sync"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

type TCPServer struct {
	emqlookupd *EMQLookupd
	conns      sync.Map
}

func (s *TCPServer) Handle(conn net.Conn) {
	log.Infof("TCP: new client(%s)", conn.RemoteAddr())

	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Infof("failed to read protocol version - %s", err)
		conn.Close()
		return
	}

	pm := string(buf)
	log.Infof("CLIENT(%s): desired protocol magic '%s'", conn.RemoteAddr(), pm)

	var prot protocol.Protocol
	switch pm {
	case common.ProtoMagic:
		prot = &LookupProtocol{emqlookupd: s.emqlookupd}
	default:
		err = protocol.SendResponse(conn, common.BadProtocolBytes)
		if err != nil {
			log.Infof("client(%s) SendResponse error: %v", conn.RemoteAddr(), err)
		}
		conn.Close()
		log.Infof("client(%s) bad protocol magic '%s'", conn.RemoteAddr(), pm)
		return
	}

	client := prot.NewClient(conn)
	s.conns.Store(conn.RemoteAddr(), client)

	err = prot.IOLoop(client)
	if err != nil {
		log.Infof("client(%s) - %s", conn.RemoteAddr(), err)
	}

	s.conns.Delete(conn.RemoteAddr())
	client.Close()
}

func (s *TCPServer) Close() {
	s.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
