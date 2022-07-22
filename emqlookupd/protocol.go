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
	case "UNREGISTER":
		return l.UNREGISTER(client, params)
	}
	return nil, fmt.Errorf("invalid command %s", params)
}

func (l *LookupProtocol) REGISTER(client *Client, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("client must IDENTIFY")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("REGISTER insufficient number of parameters")
	}

	topic := params[1]
	var channel string
	if len(params) >= 2 {
		channel = params[2]
	}

	producer := &Producer{peerInfo: client.peerInfo}
	if channel != "" {
		key := Registration{
			Category: "channel",
			Key:      topic,
			SubKey:   channel,
		}

		if l.emqlookupd.DB.AddProducer(key, producer) {
			log.Infof("DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "channel", topic, channel)
		}
	}

	key := Registration{
		Category: "topic",
		Key:      topic,
		SubKey:   "",
	}
	if l.emqlookupd.DB.AddProducer(key, producer) {
		log.Infof("DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "topic", topic, "")
	}

	return common.OKBytes, nil
}

func (l *LookupProtocol) UNREGISTER(client *Client, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("client must IDENTIFY")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("REGISTER insufficient number of parameters")
	}

	topic := params[1]
	var channel string
	if len(params) >= 2 {
		channel = params[2]
	}

	if channel != "" {
		key := Registration{
			Category: "channel",
			Key:      topic,
			SubKey:   channel,
		}

		removed, left := l.emqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			log.Infof("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s", client, "channel", topic, channel)
		}

		if left == 0 {
			l.emqlookupd.DB.RemoveRegistration(key)
		}

	} else { // 不为空删除topic下所有channel
		registrations := l.emqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			if removed, _ := l.emqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				log.Infof("client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s", client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{
			Category: "topic",
			Key:      topic,
			SubKey:   "",
		}
		removed, left := l.emqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			log.Infof("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s", client, "topic", topic, "")
		}

		if left == 0 {
			l.emqlookupd.DB.RemoveRegistration(key)
		}

	}

	return common.OKBytes, nil
}
