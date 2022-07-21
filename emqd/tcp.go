package emqd

import (
	"io"
	"net"
	"sync"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

type TCPServer struct {
	emqd  *EMQD
	conns sync.Map
}

func (t *TCPServer) Handle(conn net.Conn) {
	log.Infof("TCP: new client(%s)", conn.RemoteAddr())

	// 获取连接传过来的协议名是否正确（可以方便未来的协议升级）
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Infof("failed to read protocol version error: %v", err)
		conn.Close()
		return
	}

	// 判断协议是否正确 TODO: 这里可以通过interface{}方式支持多个协议版本
	pm := string(buf)
	if common.ProtoMagic != pm {
		log.Infof("client(%s) bad protocol magic '%s'", conn.RemoteAddr(), pm)
		conn.Close()
		return
	}
	log.Infof("CLIENT(%s): desired protocol magic '%s'", conn.RemoteAddr(), pm)

	var prot protocol.Protocol
	switch pm {
	case common.ProtoMagic:
		prot = &Protocol{emqd: t.emqd}
	default:
		protocol.SendFramedResponse(conn, common.FrameTypeError, common.BadProtocolBytes)
		conn.Close()
		log.Infof("client(%s) bad protocol magic '%s'", conn.RemoteAddr(), pm)
		return
	}

	client := prot.NewClient(conn)
	t.conns.Store(conn.RemoteAddr(), client)

	// client处理工作
	err = prot.IOLoop(client)
	if err != nil {
		log.Infof("client(%s) error: %v", conn.RemoteAddr(), err)
	}

	t.conns.Delete((conn.RemoteAddr()))
	conn.Close()
}

func (s *TCPServer) Close() {
	s.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
