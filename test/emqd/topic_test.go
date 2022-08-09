package emqd

import "testing"

func TestGetTopic(t *testing.T) {
	e := startEMQD(t)
	topicName := "test"
	topic := e.GetTopic(topicName)
	if topic == nil {
		t.Error("topic nil")
	}
	if topic.GetName() != topicName {
		t.Error("topic name error")
	}
}
