package emqd

import (
	"testing"

	"github.com/ericluj/emq/emqd"
	"github.com/stretchr/testify/assert"
)

func TestMain(t *testing.T) {
	StartEMQD(t)
}

func StartEMQD(t *testing.T) *emqd.EMQD {
	opts := emqd.NewOptions()
	e, err := emqd.NewEMQD(opts)
	assert.Nil(t, err)

	go func() {
		err = e.Main()
		assert.Nil(t, err)
	}()

	return e
}
