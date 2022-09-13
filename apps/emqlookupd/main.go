package main

import (
	"sync"
	"syscall"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/emqlookupd"
	"github.com/judwhite/go-svc"
)

type program struct {
	once       sync.Once
	emqlookupd *emqlookupd.EMQLookupd
}

func main() {
	log.SetLevel(log.DebugLevel)
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.Fatalf("Run: %v", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	opts := emqlookupd.NewOptions()
	emqlookupd, err := emqlookupd.NewEMQLookupd(opts)
	if err != nil {
		log.Fatalf("NewEMQLookupd: %v", err)
	}
	p.emqlookupd = emqlookupd
	return nil
}

func (p *program) Start() error {

	go func() {
		err := p.emqlookupd.Main()
		if err != nil {
			_ = p.Stop()
			log.Fatalf("Main: %v", err)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.emqlookupd.Exit()
	})
	return nil
}
