package example

import (
	"context"
	"fmt"
	"time"

	fnsq "github.com/DragonveinChain/FlexibleNSQ"
)

func Server() {
	config := fnsq.DefaultConfig()
	config.ConsumeAddr = "192.168.2.201:4161"
	config.ProducerAddr = "192.168.2.201:4150"
	manage := fnsq.NewManager(context.TODO(), config)

	server := manage.Server()
	go func() {
		//stop in other process
		time.Sleep(1000 * time.Second)
		manage.Stop()
	}()
	work := server.Start("server1")
	go func() {
		for {
			msg := <-work.Message()
			message := fnsq.ParseMessage(msg.Body)

			fmt.Println("msg", message, "data", string(message.Data()))
			str := "hello world server"
			server.Publisher(message.NewPublisher([]byte(str), 0))
		}
	}()

	manage.Wait()
}
