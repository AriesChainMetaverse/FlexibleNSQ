package fnsq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"

	fnsq "github.com/DragonveinChain/FlexibleNSQ"
)

var manage fnsq.Manager

func init() {
	config := fnsq.DefaultConfig()
	config.ConsumeAddr = "192.168.2.201:41601"
	config.ProducerAddr = "192.168.2.201:41600"
	manage = fnsq.NewManager(context.TODO(), config)
}

func TestManage_StartRegisterServer(t *testing.T) {
	manage.StartRegisterServer("test", func(msg *nsq.Message) error {
		fmt.Println("msg", msg)
		var wmsg fnsq.WorkMessage
		err := json.Unmarshal(msg.Body, &wmsg)
		if err != nil {
			return err
		}
		str := "hello world server"
		manage.PublishWork(fnsq.NewPublishWork(wmsg.Topic, fnsq.WorkMessage{
			Topic:  "",
			Length: len(str),
			Data:   []byte(str),
		}))
		return nil
	})
	time.Sleep(1000 * time.Second)
}

func TestManage_StartRegisterClient(t *testing.T) {
	str := "hello world"
	manage.StartWork()
	manage.RegisterClient("test2", fnsq.WorkMessage{
		Topic:  "t2",
		Length: len(str),
		Data:   []byte(str),
	}, func(msg *nsq.Message) error {
		fmt.Println(msg)
		return nil
	})
	time.Sleep(1000 * time.Second)
}
