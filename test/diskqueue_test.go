package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ericluj/emq/internal/diskqueue"
)

func TestDiskQueue(t *testing.T) {
	queue := diskqueue.New("test", "./", 10, 100000, 1, 10000, time.Second*5)
	err := queue.Put([]byte("one"))
	if err != nil {
		t.Error(err)
	}
	err = queue.Put([]byte("two"))
	if err != nil {
		t.Error(err)
	}
	err = queue.Put([]byte("three"))
	if err != nil {
		t.Error(err)
	}
	err = queue.Put([]byte("four"))
	if err != nil {
		t.Error(err)
	}

	msgChan := queue.ReadChan()
	go func(msgChan chan []byte) {
		for msg := range msgChan {
			fmt.Println(string(msg))
		}
	}(msgChan)

	time.Sleep(time.Second * 5)
}
