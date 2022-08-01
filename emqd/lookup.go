package emqd

import (
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
)

func (e *EMQD) lookupLoop() {
	var (
		lookupPeers []*LookupPeer
		lookupAddrs []string
	)
	connect := true // 是否进行连接

	ticker := time.Tick(time.Second * 15)
	for {
		if connect {
			for _, host := range e.getOpts().LookupdTCPAddresses {
				if common.InArr(host, lookupAddrs) {
					continue
				}

				log.Infof("LOOKUP(%s): adding peer", host)
				lookupPeer := NewLookupPeer(host)
				lookupPeer.Command(nil) // 开启连接
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			e.lookupPeers.Store(&lookupPeers)
			connect = false
		}

		select {
		case <-ticker:
			// 心跳
			for _, lookupPeer := range lookupPeers {
				log.Infof("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := "ping" // TODO: ping
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Infof("LOOKUPD(%s) error: %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-e.notifyChan:
			// 获取行为对应的cmd，并通知lookupd
			var (
				cmd    string // TODO: cmd
				branch string
			)
			switch val.(type) {
			case *Channel:
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() {
					cmd = "register"
				} else {
					cmd = "unregister"
				}
			case *Topic:
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() {
					cmd = "register"
				} else {
					cmd = "unregister"
				}
			}

			for _, lookupPeer := range lookupPeers {
				log.Infof("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Infof("LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-e.exitChan:
			goto exit
		}
	}

exit:
	log.Infof("LOOKUP: closing")
}
