package main

import (
	"sync"
	"syscall"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/emqd"
	"github.com/judwhite/go-svc"
)

type program struct {
	once sync.Once
	emqd *emqd.EMQD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.Fatalf("Run fatal: %v", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	opts := emqd.NewOptions()
	emqd, err := emqd.NewEMQD(opts)
	if err != nil {
		log.Fatalf("NewEMQD fatal: %v", err)
	}
	p.emqd = emqd
	return nil
}

func (p *program) Start() error {
	err := p.emqd.LoadMetadata()
	if err != nil {
		log.Fatalf("LoadMetadata fatal: %v", err)
	}

	err = p.emqd.PersistMetadata()
	if err != nil {
		log.Fatalf("PersistMetadata fatal: %v", err)
	}

	go func() {
		err := p.emqd.Main()
		if err != nil {
			_ = p.Stop()
			log.Fatalf("Main fatal: %v", err)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.emqd.Exit()
	})
	return nil
}
