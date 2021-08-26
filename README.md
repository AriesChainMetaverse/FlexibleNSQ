# FlexibleNSQ #

### Start Server ###

```go
config := fnsq.DefaultConfig()
config.ConsumeAddr = "192.168.2.201:4161"
config.ProducerAddr = "192.168.2.201:4150"
manage := fnsq.NewManager(context.TODO(), config)

server := manage.Server()
go func () {
//stop in other process
time.Sleep(1000 * time.Second)
manage.Stop()
}()
work := server.Start("server1")
go func () {
for {
msg := <-work.Message()
message := fnsq.ParseMessage(msg.Body)

fmt.Println("msg", message, "data", string(message.Data()))
str := "hello world server"
server.Publisher(message.NewPublisher([]byte(str), 0))
}
}()

manage.Wait()
```

### Start Client ###

```go
config := fnsq.DefaultConfig()
config.ConsumeAddr = "192.168.2.201:4161"
config.ProducerAddr = "192.168.2.201:4150"
manage := fnsq.NewManager(context.TODO(), config)
str := "hello world"
manage.Start()
go func () {
//stop in other process
time.Sleep(1000 * time.Second)
manage.Stop()
}()

for i := 0; i < 100; i++ {

work := manage.RegisterWorker("client1", "rnd"+strconv.Itoa(i))
manage.Publisher(work.NewPublisher(
fnsq.NewMessageData(
"rnd"+strconv.Itoa(i),
"rnd"+strconv.Itoa(i),
time.Now().UnixNano(),
[]byte(str),
)))
}

go func () {
works := manage.Workers()
//for {
for i := range works {
msg := <-works[i].Message()
message := fnsq.ParseMessage(msg.Body)

fmt.Println(message.Topic, message, "data", string(message.Data()))
}
//}
}()

manage.Wait()
```

### Stop ###

```go
//stop the service
#manage#/#server#.Stop()
```