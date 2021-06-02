package fnsq_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"

	fnsq "github.com/DragonveinChain/FlexibleNSQ"
)

var manage fnsq.Manager

func init() {
	config := fnsq.DefaultConfig()
	config.ConsumeAddr = "192.168.2.201:4161"
	config.ProducerAddr = "192.168.2.201:4150"
	manage = fnsq.NewManager(context.TODO(), config)
}

func TestManage_StartRegisterServer(t *testing.T) {
	manage.Start()
	go func() {
		time.Sleep(100 * time.Second)
		manage.Stop()
	}()
	manage.StartRegisterServer("server1", func(msg *nsq.Message) error {
		message, err := fnsq.ParseMessage(msg.Body)
		if err != nil {
			return err
		}
		fmt.Println("msg", message, "data", string(message.Data))
		str := "hello world server"
		manage.PublishWork(message.Work([]byte(str), 0))
		return nil
	})
	manage.Wait()
}

func TestManage_StartRegisterClient(t *testing.T) {
	str := "hello world"
	manage.Start()
	go func() {
		time.Sleep(100 * time.Second)
		manage.Stop()
	}()
	for i := 0; i < 100; i++ {
		manage.RegisterClient("client1", fnsq.WorkMessage{
			Topic:  "rnd" + strconv.Itoa(i),
			Length: len(str),
			Data:   []byte(str),
		}, func(msg *nsq.Message) error {
			message, err := fnsq.ParseMessage(msg.Body)
			if err != nil {
				return err
			}
			fmt.Println(message.Topic, message, "data", string(message.Data))
			return nil
		})
	}
	manage.Wait()
}
