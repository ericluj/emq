package emqd

import (
	"testing"

	"github.com/ericluj/emq/emqd"
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
	if outputMsg.ID != id {
		t.Error("id error")
	}
	if string(outputMsg.Body) != body {
		t.Error("id error")
	}
}
