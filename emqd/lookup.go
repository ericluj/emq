package emqd

import (
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/util"
)

func (e *EMQD) lookupLoop() {
	var (
		lookupPeers []*LookupPeer
		lookupAddrs []string
	)
	connect := true // 是否进行连接
	heartbeatTicker := time.NewTicker(common.HeartbeatTimeout)
	for {
		if connect {
			for _, addr := range e.getOpts().LookupdTCPAddresses {
				if util.InArr(addr, lookupAddrs) {
					continue
				}

				log.Infof("lookup: %s, adding peer", addr)
				lookupPeer := NewLookupPeer(addr)
				err := lookupPeer.Connect(e)
				if err != nil {
					log.Infof("Connect error: %v", err)
					// TODO: 是否要抛error
					goto exit
				}
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, addr)
			}
			e.lookupPeers.Store(&lookupPeers)
			connect = false
		}
		select {
		case <-heartbeatTicker.C:
			// 心跳
			for _, lp := range lookupPeers {
				log.Infof("lookup: %s, sending heartbeat", lp.addr)
				cmd := command.PingCmd()
				_, err := lp.Command(cmd)
				if err != nil {
					log.Infof("lookup: %s, cmd: %s, error: %v", lp.addr, cmd, err)
				}
			}
		case val := <-e.notifyChan:
			// 获取行为对应的cmd，并通知lookupd
			var (
				cmd    *command.Command
				branch string
			)
			switch v := val.(type) {
			case *Channel:
				branch = "channel"
				channel := v
				if channel.Exiting() {
					cmd = command.UnRegisterCmd(channel.topicName, channel.name)
				} else {
					cmd = command.RegisterCmd(channel.topicName, channel.name)
				}
			case *Topic:
				branch = "topic"
				topic := v
				if topic.Exiting() {
					cmd = command.UnRegisterCmd(topic.name, "")
				} else {
					cmd = command.RegisterCmd(topic.name, "")
				}
			}

			for _, lp := range lookupPeers {
				log.Infof("lookup: %s, branch: %s, cmd: %s", lp.addr, branch, cmd)
				_, err := lp.Command(cmd)
				if err != nil {
					log.Infof("lookup: %s, branch: %s, cmd: %s, eror: %v", lp.addr, branch, cmd, err)
				}
			}
		case <-e.exitChan:
			goto exit
		}
	}

exit:
	log.Infof("lookupLoop: exit")
	heartbeatTicker.Stop()
}
