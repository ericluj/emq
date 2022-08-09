package emqd

import (
	"testing"

	"github.com/ericluj/emq/emqd"
	"github.com/stretchr/testify/assert"
)

func TestPutMessage(t *testing.T) {
	e := StartEMQD(t)
	topicName := "test"
	topic := e.GetTopic(topicName)

	channelName := "ch"
	channel := topic.GetChannel(channelName)

	var id emqd.MessageID
	body := "one"
	msg := emqd.NewMessage(id, []byte(body))
	_ = topic.PutMessage(msg)

	outputMsg := <-channel.GetMemoryMsgChan()
	assert.Equal(t, id, outputMsg.ID)
	assert.Equal(t, body, outputMsg.Body)
}
