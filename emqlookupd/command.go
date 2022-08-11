package emqlookupd

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
)

func (l *LookupProtocol) PING(client *Client, params [][]byte) ([]byte, error) {
	if client.peerInfo != nil {
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		log.Infof("CLIENT(%v): pinged (last ping %s)", client.peerInfo.id, now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return common.OKBytes, nil
}

func (l *LookupProtocol) IDENTIFY(client *Client, params [][]byte) ([]byte, error) {
	var err error
	if client.peerInfo != nil {
		return nil, fmt.Errorf("can not IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(client.conn, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.conn, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body")
	}

	peerInfo := PeerInfo{id: client.conn.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.conn.RemoteAddr().String()

	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 {
		return nil, fmt.Errorf("IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	log.Infof("CLIENT(%v): IDENTIFY Address:%s TCP:%d HTTP:%d", client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort)

	client.peerInfo = &peerInfo

	if l.emqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		log.Infof("DB: client(%v) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	data := make(map[string]interface{})
	data["tcp_port"] = l.emqlookupd.tcpListener.Addr().(*net.TCPAddr)
	data["http_port"] = l.emqlookupd.httpListener.Addr().(*net.TCPAddr)

	resp, err := json.Marshal(data)
	if err != nil {
		log.Infof("marshaling %v", data)
		return common.OKBytes, nil
	}

	return resp, nil
}

func (l *LookupProtocol) REGISTER(client *Client, params [][]byte) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("client must IDENTIFY")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("REGISTER insufficient number of parameters")
	}

	topic := string(params[1])
	var channel string
	if len(params) >= 2 {
		channel = string(params[2])
	}

	producer := &Producer{peerInfo: client.peerInfo}
	if channel != "" {
		key := Registration{
			Category: "channel",
			Key:      topic,
			SubKey:   channel,
		}

		if l.emqlookupd.DB.AddProducer(key, producer) {
			log.Infof("DB: client(%v) REGISTER category:%s key:%s subkey:%s", client, "channel", topic, channel)
		}
	}

	key := Registration{
		Category: "topic",
		Key:      topic,
		SubKey:   "",
	}
	if l.emqlookupd.DB.AddProducer(key, producer) {
		log.Infof("DB: client(%v) REGISTER category:%s key:%s subkey:%s", client, "topic", topic, "")
	}

	return common.OKBytes, nil
}

func (l *LookupProtocol) UNREGISTER(client *Client, params [][]byte) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("client must IDENTIFY")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("REGISTER insufficient number of parameters")
	}

	topic := string(params[1])
	var channel string
	if len(params) >= 2 {
		channel = string(params[2])
	}

	if channel != "" {
		key := Registration{
			Category: "channel",
			Key:      topic,
			SubKey:   channel,
		}

		removed, left := l.emqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			log.Infof("DB: client(%v) UNREGISTER category:%s key:%s subkey:%s", client, "channel", topic, channel)
		}

		if left == 0 {
			l.emqlookupd.DB.RemoveRegistration(key)
		}

	} else { // 不为空删除topic下所有channel
		registrations := l.emqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			if removed, _ := l.emqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				log.Infof("client(%v) unexpected UNREGISTER category:%s key:%s subkey:%s", client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{
			Category: "topic",
			Key:      topic,
			SubKey:   "",
		}
		removed, left := l.emqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			log.Infof("DB: client(%v) UNREGISTER category:%s key:%s subkey:%s", client, "topic", topic, "")
		}

		if left == 0 {
			l.emqlookupd.DB.RemoveRegistration(key)
		}

	}

	return common.OKBytes, nil
}
