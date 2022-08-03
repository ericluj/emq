package emqd

import (
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/util"
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
				if util.InArr(host, lookupAddrs) {
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
				cmd := command.PingCmd()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Infof("LOOKUPD(%s) error: %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-e.notifyChan:
			// 获取行为对应的cmd，并通知lookupd
			var (
				cmd    *command.Command
				branch string
			)
			switch val.(type) {
			case *Channel:
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() {
					cmd = command.RegisterCmd(channel.topicName, channel.name)
				} else {
					cmd = command.UnRegisterCmd(channel.topicName, channel.name)
				}
			case *Topic:
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() {
					cmd = command.RegisterCmd(topic.name, "")
				} else {
					cmd = command.UnRegisterCmd(topic.name, "")
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
