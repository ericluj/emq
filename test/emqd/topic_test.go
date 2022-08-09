package emqd

import (
	"testing"
)

func TestGetTopic(t *testing.T) {
	e := StartEMQD(t)
	topicName := "test"
	topic := e.GetTopic(topicName)
	if topic == nil {
		t.Error("topic nil")
	}
	if topic.GetName() != topicName {
		t.Error("topic name error")
	}
}

func TestGetChannel(t *testing.T) {
	e := StartEMQD(t)
	topicName := "test"
	topic := e.GetTopic(topicName)

	channelName := "ch"
	channel := topic.GetChannel(channelName)
	if channel == nil {
		t.Error("channel nil")
	}
	if channel.GetName() != channelName {
		t.Error("channel name error")
	}
}
