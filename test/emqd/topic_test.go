package emqd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTopic(t *testing.T) {
	e := StartEMQD(t)
	topicName := "test"
	topic := e.GetTopic(topicName)
	assert.NotNil(t, e)
	assert.Equal(t, topicName, topic.GetName())
}

func TestGetChannel(t *testing.T) {
	e := StartEMQD(t)
	topicName := "test"
	topic := e.GetTopic(topicName)

	channelName := "ch"
	channel := topic.GetChannel(channelName)
	assert.NotNil(t, e)
	assert.Equal(t, channelName, channel.GetName())
}
