package test

import (
	"testing"

	"github.com/ericluj/emq/emqd"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestGetTopic(t *testing.T) {
	topicName := "test"
	topic := e.GetTopic(topicName)
	assert.NotNil(t, e)
	assert.Equal(t, topicName, topic.GetName())
}

func TestGetChannel(t *testing.T) {
	topicName := "test"
	topic := e.GetTopic(topicName)

	channelName := "ch"
	channel := topic.GetChannel(channelName)
	assert.NotNil(t, e)
	assert.Equal(t, channelName, channel.GetName())
}

func TestPutMessage(t *testing.T) {
	topicName := "test"
	topic := e.GetTopic(topicName)

	channelName := "ch"
	channel := topic.GetChannel(channelName)

	var id protocol.MessageID
	body := "one"
	msg := emqd.NewMessage(id, []byte(body))
	_ = topic.PutMessage(msg)

	outputMsg := <-channel.GetMemoryMsgChan()
	assert.Equal(t, id, outputMsg.ID)
	assert.Equal(t, body, string(outputMsg.Body))
}
