package fnsq

import (
	"sync"

	"github.com/nsqio/go-nsq"
)

var DefaultRegisterName = "register"

type manage struct {
	nsqConfig       *nsq.Config
	registerName    string
	producerAddr    string //"127.0.0.1:4150"
	consumeAddr     string //"127.0.0.1:4160"
	workerLock      sync.RWMutex
	workers         map[string]Work
	interactionChan *interactionChan
}

func (m *manage) RegisterName() string {
	return m.registerName
}

func (m *manage) SetRegisterName(registerName string) {
	m.registerName = registerName
}

func (m *manage) NsqConfig() *nsq.Config {
	return m.nsqConfig
}

func (m *manage) SetNsqConfig(nsqConfig *nsq.Config) {
	m.nsqConfig = nsqConfig
}

func (m *manage) RegistryWorker(work Work) Work {
	m.workerLock.Lock()
	m.workers[work.Topic()] = work
	m.workerLock.Unlock()
	return work
}

func (m *manage) Work(topic string) (Work, bool) {
	m.workerLock.RLock()
	work, exist := m.workers[topic]
	m.workerLock.RUnlock()
	return work, exist
}

func (m *manage) DestroyWork(work Work) {
	m.workerLock.Lock()
	delete(m.workers, work.Topic())
	m.workerLock.Unlock()
}

func (m *manage) Works() []Work {
	var works []Work
	m.workerLock.Lock()
	for i := range m.workers {
		works = append(works, m.workers[i])
	}
	m.workerLock.Unlock()
	return works
}

func (m *manage) consumeWorker(work Work) error {
	consumer, err := nsq.NewConsumer(work.Topic(), work.Channel(), m.nsqConfig)
	if err != nil {
		return err
	}
	consumer.AddHandler(work)
	err = consumer.ConnectToNSQLookupd(m.consumeAddr)
	if err != nil {
		return err
	}
	return nil
}

func (m *manage) Publish(interaction Interaction) {
	m.interactionChan.In <- interaction
}

func (m *manage) StartRegisterServer(channel string, fn WorkActionFunc) {
	work, b := m.Work(DefaultRegisterName)
	if b {
		return
	}
	work = NewWork(Interaction{
		Topic:   DefaultRegisterName,
		Channel: channel,
	}, fn)
	m.RegistryWorker(work)
	go m.consumeWorker(work)
}

func (m *manage) StartRegisterClient() {

}

func (m *manage) produceWorker() error {
	producer, err := nsq.NewProducer(m.producerAddr, m.nsqConfig)
	if err != nil {
		return err
	}
	defer producer.Stop()
	//var producerData string
	//var interaction Interaction
	for {
		errPing := producer.Ping()
		if errPing != nil {
			break
		}

		//select {
		//case producerData = <-m.producerChan.Out:
		//interaction, err = ParseInteraction(producerData)
		//if err != nil {
		//	continue
		//}
		//err = producer.Publish(interaction.Topic, interaction.Data)
		//if err != nil {
		//	continue
		//}
		//}
	}
	return nil
}

func initManage() *manage {
	return &manage{
		registerName:    DefaultRegisterName,
		nsqConfig:       nsq.NewConfig(),
		producerAddr:    "",
		interactionChan: NewinteractionChan(5),
	}
}
