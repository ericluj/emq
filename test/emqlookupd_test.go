package test

import (
	"net"
	"testing"
	"time"

	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
)

func TestProtocol(t *testing.T) {
	conn, err := net.DialTimeout("tcp", l.GetOpts().TCPAddress, common.DialTimeout)
	if err != nil {
		t.Error(err)
	}

	ping := command.PingCmd()
	err = ping.Write(conn)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second * 5)
}
