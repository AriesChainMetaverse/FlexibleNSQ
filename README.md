# FlexibleNSQ #

### Start Server ###

```go
config := fnsq.DefaultConfig()
config.ConsumeAddr = "127.0.0.1:4161"
config.ProducerAddr = "127.0.0.1:4150"
manage = fnsq.NewManager(context.TODO(), config)
manage.Start()

work := manage.RegisterServer("server1")
go func () {
    for {
        msg := <-work.Message()
        message, err := fnsq.ParseMessage(msg.Body)
        if err != nil {
            return
        }
    if message.ID == "" {
        continue
    }
    fmt.Println("msg", message, "data", string(message.Data))
    str := "hello world server"

    manage.PublishWorker(message.Work([]byte(str), 0))
    }
}()

manage.Wait()
```

### Start Client ###

```go
config := fnsq.DefaultConfig()
config.ConsumeAddr = "127.0.0.1:4161"
config.ProducerAddr = "127.0.0.1:4150"
manage = fnsq.NewManager(context.TODO(), config)
manage.Start()

manage.RegisterClient("client1", fnsq.WorkMessage{
    ID:     "client1",
    Topic:  "rnd" + strconv.Itoa(i),
    Length: len(str),
    Data:   []byte(str),
})


go func () {
    works := manage.Workers()
        for i := range works {
            msg := <-works[i].Message()
            message, err := fnsq.ParseMessage(msg.Body)
            if err != nil {
                return
        }
        fmt.Println(message.Topic, message, "data", string(message.Data))
    }
}()

manage.Wait()
```

### Stop ###

```go
//stop the manage
manage.Stop()
```