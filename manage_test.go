package fnsq_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	fnsq "github.com/DragonveinChain/FlexibleNSQ"
)

var manage fnsq.Manager

func init() {
	config := fnsq.DefaultConfig()
	config.ConsumeAddr = "127.0.0.1:4161"
	config.ProducerAddr = "127.0.0.1:4150"
	config.Interval = 3
	manage = fnsq.NewManager(context.TODO(), config)

	fnsq.DEBUG = true
}

func TestManage_StartRegisterServer(t *testing.T) {
	fmt.Printf("nsqconfig:%+v\n", manage.NSQConfig())
	manage.Start()
	go func() {
		time.Sleep(100 * time.Second)
		manage.Stop()
	}()
	work := manage.RegisterServer("server1")
	go func() {
		for {
			msg := <-work.Message()
			if string(msg.Body) == fnsq.HelloWorld {
				fmt.Println("new hello world")
				continue
			}
			message := fnsq.ParseMessage(msg.Body)
			fmt.Println("received server message:", message)
			if message.ID() == "" {
				continue
			}
			fmt.Println("msg", message, "data", string(message.Data()))
			str := "hello world server"

			manage.PublishWorker(message.NewWork([]byte(str), 0))
		}
	}()

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
		manage.RegisterClient("rnd"+strconv.Itoa(i), "client1", fnsq.NewMessageData(
			"client1",
			"rnd"+strconv.Itoa(i),
			time.Now().UnixNano(),
			[]byte(str),
		))
	}

	go func() {
		works := manage.Workers()
		//for {
		for i := range works {
			msg := <-works[i].Message()
			message := fnsq.ParseMessage(msg.Body)

			fmt.Println("receive new message:", message.Topic(), "data", string(message.Data()))
		}
		//}
	}()

	manage.Wait()
}
