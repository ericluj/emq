package emqlookupd

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

type LookupProtocol struct {
	emqlookupd *EMQLookupd
}

func (l *LookupProtocol) NewClient(conn net.Conn) protocol.Client {
	return &Client{
		Conn:   conn,
		Reader: bufio.NewReaderSize(conn, common.DefaultBufferSize),
		Writer: bufio.NewWriterSize(conn, common.DefaultBufferSize),
	}
}

func (l *LookupProtocol) IOLoop(c protocol.Client) error {
	var (
		err  error
		line string
	)

	client := c.(*Client)

	for {
		line, err = client.Reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		var response []byte
		response, err = l.Exec(client, params)
		if err != nil {
			// TODO: 处理内部error
		}

		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	log.Infof("PROTOCOL: [%s] exiting ioloop", client.RemoteAddr())

	return nil
}

func (l *LookupProtocol) Exec(client *Client, params []string) ([]byte, error) {
	switch params[0] {
	case "REGISTER":
		return l.REGISTER(client, params)
	}
	return nil, fmt.Errorf("invalid command %s", params[0])
}

func (l *LookupProtocol) REGISTER(client *Client, params []string) ([]byte, error) {
	return common.OKBytes, nil
}
