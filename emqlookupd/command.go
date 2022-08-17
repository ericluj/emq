package emqlookupd

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

func (l *LookupProtocol) PING(client *Client, params [][]byte) ([]byte, error) {
	if client.peerInfo != nil {
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		log.Infof("client: %s, pinged (last ping %s)", client.peerInfo.id, now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return common.OKBytes, nil
}

func (l *LookupProtocol) IDENTIFY(client *Client, params [][]byte) ([]byte, error) {
	var err error
	if client.peerInfo != nil {
		return nil, fmt.Errorf("IDENTITY: can not again")
	}

	bodyLen, err := protocol.ReadDataSize(client.conn)
	if err != nil {
		return nil, fmt.Errorf("IDENTITY: failed to read data size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.conn, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTITY: failed to read body")
	}

	// 构造信息
	peerInfo := PeerInfo{id: client.conn.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY: failed to decode JSON body")
	}

	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 {
		return nil, fmt.Errorf("IDENTIFY: missing fields")
	}

	peerInfo.RemoteAddress = client.conn.RemoteAddr().String()
	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	client.peerInfo = &peerInfo

	log.Infof("IDENTIFY: client: %s, Address:%s, TCP: %d, HTTP: %d", peerInfo.id, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort)

	// 保存数据
	producer := &Producer{peerInfo: client.peerInfo}
	reg := Registration{
		Category: CatagoryClient,
	}
	if l.emqlookupd.DB.AddProducer(reg, producer) {
		log.Infof("IDENTIFY: AddProducer client: %s, category: %s, key: %s, subkey: %s", client.peerInfo.id, reg.Category, reg.Key, reg.SubKey)
	}

	data := make(map[string]interface{})
	data["tcp_port"] = l.emqlookupd.tcpListener.Addr().(*net.TCPAddr)
	data["http_port"] = l.emqlookupd.httpListener.Addr().(*net.TCPAddr)

	resp, err := json.Marshal(data)
	if err != nil {
		log.Infof("Marshal error: %v, data: %v", err, data)
		return common.OKBytes, nil
	}

	return resp, nil
}

func (l *LookupProtocol) REGISTER(client *Client, params [][]byte) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("REGISTER: client must IDENTIFY")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("REGISTER: insufficient number of parameters")
	}

	topic := string(params[1])
	var channel string
	if len(params) >= 2 {
		channel = string(params[2])
	}

	producer := &Producer{peerInfo: client.peerInfo}

	// 注册topic
	reg := Registration{
		Category: CatagoryTopic,
		Key:      topic,
	}
	if l.emqlookupd.DB.AddProducer(reg, producer) {
		log.Infof("REGISTER: AddProducer client: %s, category: %s, key: %s, subkey: %s", client.peerInfo.id, reg.Category, reg.Key, reg.SubKey)
	}

	// 注册channel
	if channel != "" {
		reg := Registration{
			Category: CatagoryChannel,
			Key:      topic,
			SubKey:   channel,
		}

		if l.emqlookupd.DB.AddProducer(reg, producer) {
			log.Infof("REGISTER: AddProducer client: %s, category: %s, key: %s, subkey: %s", client.peerInfo.id, reg.Category, reg.Key, reg.SubKey)
		}
	}

	return common.OKBytes, nil
}

func (l *LookupProtocol) UNREGISTER(client *Client, params [][]byte) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("UNREGISTER: client must IDENTIFY")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("UNREGISTER: insufficient number of parameters")
	}

	topic := string(params[1])
	var channel string
	if len(params) >= 2 {
		channel = string(params[2])
	}

	if channel != "" { // 注销channel
		reg := Registration{
			Category: CatagoryChannel,
			Key:      topic,
			SubKey:   channel,
		}

		removed, left := l.emqlookupd.DB.RemoveProducer(reg, client.peerInfo.id)
		if removed {
			log.Infof("UNREGISTER: RemoveProducer client: %s, category: %s, key: %s, subkey: %s", client.peerInfo.id, reg.Category, reg.Key, reg.SubKey)
		}

		if left == 0 {
			l.emqlookupd.DB.RemoveRegistration(reg)
		}

	} else { // 注销topic
		registrations := l.emqlookupd.DB.FindRegistrations(CatagoryChannel, topic, "*")
		for _, reg := range registrations {
			removed, left := l.emqlookupd.DB.RemoveProducer(reg, client.peerInfo.id)
			if removed {
				log.Infof("UNREGISTER: RemoveProducer client: %s, category: %s, key: %s, subkey: %s", client.peerInfo.id, reg.Category, reg.Key, reg.SubKey)
			}
			if left == 0 {
				l.emqlookupd.DB.RemoveRegistration(reg)
			}
		}

		reg := Registration{
			Category: CatagoryTopic,
			Key:      topic,
		}
		removed, left := l.emqlookupd.DB.RemoveProducer(reg, client.peerInfo.id)
		if removed {
			log.Infof("UNREGISTER: RemoveProducer client: %s, category: %s, key: %s, subkey: %s", client.peerInfo.id, reg.Category, reg.Key, reg.SubKey)
		}

		if left == 0 {
			l.emqlookupd.DB.RemoveRegistration(reg)
		}

	}

	return common.OKBytes, nil
}
