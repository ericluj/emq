package test

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

func TestProtocol(t *testing.T) {
	go server(t)
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6666", common.DialTimeout)
	if err != nil {
		t.Error(err)
	}

	data := map[string]interface{}{
		"one": "111",
	}
	identify, err := command.IDENTIFYCmd(data)
	if err != nil {
		t.Error(err)
	}
	err = identify.Write(conn)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second * 10)
}

func server(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:6666")
	if err != nil {
		t.Error(err)
	}

	err = protocol.TCPServer(listener, &Hand{})
	if err != nil {
		t.Error(err)
	}
}

type Hand struct{}

func (h *Hand) Handle(conn net.Conn) {
	var (
		err  error
		line []byte
	)

	// 设置deadline
	err = conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
	if err != nil {
		fmt.Println(err)
	}

	// 接受cmd并执行操作
	reader := bufio.NewReaderSize(conn, common.DefaultBufferSize)
	for {
		line, err = reader.ReadSlice('\n')
		if err != nil {
			break
		}

		// 去掉'\n'（返回的line包含'\n'）
		line = line[:len(line)-1]
		// 去掉'\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// 空格拆分
		params := bytes.Split(line, common.SeparatorBytes)
		log.Infof("PROTOCOL: %s, params: %s", conn.RemoteAddr(), params)

		if string(params[0]) == command.IDENTIFY {
			fmt.Println("identify read data...")
			bs, err := protocol.ReadData(reader)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(bs))
		}
	}
}
