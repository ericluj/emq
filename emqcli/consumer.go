package emqcli

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/util"
	"github.com/go-resty/resty/v2"
)

var instCount int64

var ErrAlreadyConnected = errors.New("already connected")

type Consumer struct {
	mtx               sync.RWMutex
	wg                util.WaitGroup
	lookupdAddrs      []string
	lookupdQueryIndex int
	lookupClient      *resty.Client
	id                int64
	topic             string
	channel           string

	conns      map[string]*Conn
	msgChan    chan *Message
	exitChan   chan int
	isExisting int32

	exitOnce sync.Once
}

type Handler interface {
	HandleMessage(message *Message) error
}

func NewConsumer(topic, channel string) *Consumer {
	c := &Consumer{
		id:       atomic.AddInt64(&instCount, 1),
		topic:    topic,
		channel:  channel,
		conns:    make(map[string]*Conn),
		msgChan:  make(chan *Message),
		exitChan: make(chan int),
	}
	return c
}

func (co *Consumer) GetConns() []*Conn {
	co.mtx.RLock()
	defer co.mtx.RUnlock()

	conns := make([]*Conn, 0, len(co.conns))
	for _, v := range co.conns {
		conns = append(conns, v)
	}

	return conns
}

func (co *Consumer) Stop() {
	co.exitOnce.Do(func() {
		atomic.StoreInt32(&co.isExisting, 1)

		close(co.exitChan)

		conns := co.GetConns()
		for _, v := range conns {
			v.Stop()
		}
	})
}

func (co *Consumer) OnClose(conn *Conn) {
	co.mtx.Lock()
	delete(co.conns, conn.addr)
	co.mtx.Unlock()
}

func (co *Consumer) OnRequeue(msg *Message) {
	cmd := command.RequeueCmd(msg.ID[:])
	if err := msg.conn.Command(cmd); err != nil {
		log.Infof("OnRequeue error: %v", err)
		co.Stop()
	}
}

func (co *Consumer) AddHandler(handler Handler) {
	go co.handlerLoop(handler)
}

func (co *Consumer) handlerLoop(handler Handler) {
	for {
		select {
		case msg := <-co.msgChan:
			err := handler.HandleMessage(msg)
			if err != nil {
				log.Infof("HandleMessage error: %v", err)
				msg.delegate = co
				msg.Requeue()

				continue
			}
		case <-co.exitChan:
			goto exit
		}
	}

exit:
	log.Infof("handlerLoop exit")
}

func (co *Consumer) Existing() bool {
	return atomic.LoadInt32(&co.isExisting) == 1
}

func (co *Consumer) ConnectToEMQD(addr string) error {
	if co.Existing() {
		return fmt.Errorf("consumer can not connect")
	}

	co.mtx.Lock()
	if _, ok := co.conns[addr]; ok {
		co.mtx.Unlock()
		return ErrAlreadyConnected
	}
	co.mtx.Unlock()

	conn := NewConn(addr, co.msgChan, co)
	if err := conn.Connect(); err != nil {
		co.Stop()
		return err
	}

	cmd := command.SubscribeCmd(co.topic, co.channel)
	if err := conn.Command(cmd); err != nil {
		co.Stop()
		return fmt.Errorf("SubscribeCmd error: %v", err)
	}

	co.mtx.Lock()
	co.conns[addr] = conn
	co.mtx.Unlock()

	log.Infof("ConnectToEMQD %s", addr)

	return nil
}

func (co *Consumer) ConnectToLookupd(addr string) error {
	if co.Existing() {
		return fmt.Errorf("consumer can not connect")
	}

	co.mtx.Lock()
	// 判重
	for _, v := range co.lookupdAddrs {
		if v == addr {
			co.mtx.Unlock()
			return nil
		}
	}

	co.lookupdAddrs = append(co.lookupdAddrs, addr)
	if co.lookupClient == nil {
		co.lookupClient = resty.New()
	}
	numlookupd := len(co.lookupdAddrs)
	co.mtx.Unlock()

	// 如果是第一个，那么启动循环
	if numlookupd == 1 {
		co.queryLookupd()         // 先查询一次
		co.wg.Wrap(co.lookupLoop) // 定时器循环查询
	}

	return nil
}

func (co *Consumer) lookupLoop() {
	ticker := time.NewTicker(common.HeartbeatTimeout)

	for {
		select {
		case <-ticker.C:
			co.queryLookupd()
		case <-co.exitChan:
			goto exit
		}
	}

exit:
	ticker.Stop()
	log.Infof("lookupLoop exit")
}

func (co *Consumer) nextLookupdEndpoint() string {
	co.mtx.RLock()
	if co.lookupdQueryIndex >= len(co.lookupdAddrs) {
		co.lookupdQueryIndex = 0
	}
	addr := co.lookupdAddrs[co.lookupdQueryIndex]
	co.mtx.RUnlock()
	co.lookupdQueryIndex = (co.lookupdQueryIndex + 1) % len(co.lookupdAddrs)

	return addr
}

func (co *Consumer) queryLookupd() {
	retryNum := 0

retry:
	endpoint := co.nextLookupdEndpoint()
	log.Infof("queryLookupd %s", endpoint)
	req := map[string]string{
		"topic": co.topic,
	}
	url := "http://" + endpoint + "/lookup"
	resp, err := co.lookupClient.R().SetQueryParams(req).Get(url)
	if err != nil {
		log.Infof("queryLookupd error: %v, endpoint: %s", err, endpoint)
		retryNum++
		if retryNum < 3 {
			log.Infof("retry queryLookupd")
			goto retry
		}
		return
	}

	data := lookupResp{}
	if err := json.Unmarshal(resp.Body(), &data); err != nil {
		log.Infof("queryLookupd Unmarshal error%v", err)
		return
	}

	for _, v := range data.Producers {
		err := co.ConnectToEMQD(v.TCPAddress)
		if err != nil && err != ErrAlreadyConnected {
			log.Infof("ConnectToEMQD error: %v", err)
			continue
		}
	}

}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
}

type peerInfo struct {
	TCPAddress       string `json:"tcp_address"`
	HTTPAddress      string `json:"http_address"`
	BroadcastAddress string `json:"broadcast_address"`
}
