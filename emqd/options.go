package emqd

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/util"
)

type Options struct {
	ID                  int64
	TCPAddress          string
	HTTPAddress         string
	LookupdTCPAddresses []string
	DataPath            string
	MemQueueSize        int64
	MinMsgSize          int64
	MaxMsgSize          int64
	MaxBytesPerFile     int64
	SyncEvery           int64
	SyncTimeout         time.Duration
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("NewOptions: %v", err)
	}

	h := md5.New()
	_, err = io.WriteString(h, hostname)
	if err != nil {
		log.Errorf("WriteString: %v", err)
	}
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
	o := &Options{
		ID:          defaultID,
		TCPAddress:  "0.0.0.0:6001",
		HTTPAddress: "0.0.0.0:6002",
		LookupdTCPAddresses: []string{
			"127.0.0.1:7001",
		},
		DataPath:        "./tmp",
		MemQueueSize:    10000,
		MinMsgSize:      common.MinValidMsgLength,
		MaxMsgSize:      common.MinValidMsgLength + 1024*1024,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2000,
		SyncTimeout:     2 * time.Second,
	}
	if util.IsDocker() {
		o.TCPAddress = fmt.Sprintf("%s:6001", util.GetDockerHost())
		o.HTTPAddress = fmt.Sprintf("%s:6002", util.GetDockerHost())
		o.LookupdTCPAddresses = []string{
			"emqlookupd1:7001",
			"emqlookupd2:7001",
		}
	}
	return o
}
