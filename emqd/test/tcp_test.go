package test

import (
	"emq/emqd"
	"net"
	"testing"
	"time"
)

// 测试tcp连接
func TestTcp(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6001", time.Second)
	if err != nil {
		return
	}
	conn.Write([]byte(emqd.ProtoMagic))
}
