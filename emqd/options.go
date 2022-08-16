package emqd

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"os"

	log "github.com/ericluj/elog"
)

type Options struct {
	ID                  int64
	TCPAddress          string
	HTTPAddress         string
	MemQueueSize        int64
	MaxMsgSize          int64
	LookupdTCPAddresses []string
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("NewOptions fatal: %v", err)
	}

	h := md5.New()
	_, err = io.WriteString(h, hostname)
	if err != nil {
		log.Infof("error: %v", err)
	}
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
	return &Options{
		ID:           defaultID,
		TCPAddress:   "0.0.0.0:6001",
		HTTPAddress:  "0.0.0.0:6002",
		MemQueueSize: 10000,
		MaxMsgSize:   1024 * 1024,
	}
}
