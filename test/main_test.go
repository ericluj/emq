package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/ericluj/emq/emqd"
	"github.com/ericluj/emq/emqlookupd"
)

var (
	l *emqlookupd.EMQLookupd
	e *emqd.EMQD
)

func TestMain(m *testing.M) {
	l = StartEMQLookupd()
	e = StartEMQD()

	os.Exit(m.Run())
}

func StartEMQD() *emqd.EMQD {
	opts := emqd.NewOptions()
	e, err := emqd.NewEMQD(opts)
	if err != nil {
		fmt.Println(err)
	}

	go func() {
		err = e.Main()
		if err != nil {
			fmt.Println(err)
		}
	}()

	return e
}

func StartEMQLookupd() *emqlookupd.EMQLookupd {
	opts := emqlookupd.NewOptions()
	l, err := emqlookupd.NewEMQLookupd(opts)
	if err != nil {
		fmt.Println(err)
	}

	go func() {
		err = l.Main()
		if err != nil {
			fmt.Println(err)
		}
	}()

	return l
}
