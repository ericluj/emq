package emqd

import (
	"testing"

	"github.com/ericluj/emq/emqd"
)

func TestMain(t *testing.T) {
	startEMQD(t)
}

func startEMQD(t *testing.T) *emqd.EMQD {
	opts := emqd.NewOptions()
	e, err := emqd.NewEMQD(opts)
	if err != nil {
		t.Error(err)
	}

	go func() {
		err = e.Main()
		if err != nil {
			t.Error(err)
		}
	}()

	return e
}
