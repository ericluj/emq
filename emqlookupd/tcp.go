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

func (t *TCPServer) Handle(conn net.Conn) {
	log.Infof("TCP: new client %s", conn.RemoteAddr())

	// 获取连接传过来的协议名是否正确（可以方便未来的协议升级）
	buf := make([]byte, common.ProtoMagicLen)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Errorf("ReadFull: %v", err)
		conn.Close()
		return
	}

	// 判断协议是否正确
	pm := string(buf)
	if pm != common.ProtoMagic {
		log.Infof("client %s: bad protocol magic '%s'", conn.RemoteAddr(), pm)
		conn.Close()
		return
	}
	log.Infof("client %s: desired protocol magic '%s'", conn.RemoteAddr(), pm)

	var prot protocol.Protocol
	switch pm {
	case common.ProtoMagic:
		prot = &LookupProtocol{emqlookupd: t.emqlookupd}
	default:
		err := protocol.SendFrameData(conn, common.FrameTypeError, common.BadProtocolBytes)
		if err != nil {
			log.Errorf("SendFrameData: %v", err)
		}
		conn.Close()
		log.Infof("client %s: bad protocol magic '%s'", conn.RemoteAddr(), pm)
		return
	}

	client := prot.NewClient(conn)
	t.conns.Store(conn.RemoteAddr(), client)

	// client处理工作
	err = prot.IOLoop(client)
	if err != nil {
		log.Errorf("IOLoop: %v, client: %s", err, conn.RemoteAddr())
	}

	t.conns.Delete(conn.RemoteAddr())
	client.Close()
}

func (t *TCPServer) Close() {
	t.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
