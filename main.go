package main

import (
	"sync"
	"syscall"

	log "github.com/ericluj/elog"
	"github.com/judwhite/go-svc"
)

type program struct {
	wg   sync.WaitGroup
	quit chan struct{}
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		log.Fatalf("svc run fatal: %v", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	log.Infof("Init completed")
	return nil
}

func (p *program) Start() error {
	p.quit = make(chan struct{})

	p.wg.Add(1)
	go func() {
		log.Infof("Starting...")
		<-p.quit
		log.Infof("Quit signal received...")
		p.wg.Done()
	}()

	return nil
}

func (p *program) Stop() error {
	log.Infof("Stopping...")
	close(p.quit)
	p.wg.Wait()
	log.Infof("Stopped.")
	return nil
}
