package fnsq

import (
	"context"
	"sync"

	"github.com/nsqio/go-nsq"
)

const DefaultRegisterName = "register"

type manage struct {
	ctx          context.Context
	nsqConfig    *nsq.Config
	registerName string
	producerAddr string //"127.0.0.1:4150"
	consumeAddr  string //"127.0.0.1:4160"
	workerLock   sync.RWMutex
	workers      map[string]Work
	workChan     *WorkChan
}

func (m *manage) ProducerAddr() string {
	return m.producerAddr
}

func (m *manage) SetProducerAddr(producerAddr string) {
	m.producerAddr = producerAddr
}

func (m *manage) ConsumeAddr() string {
	return m.consumeAddr
}

func (m *manage) SetConsumeAddr(consumeAddr string) {
	m.consumeAddr = consumeAddr
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

	work.AddConsumer(consumer)
	consumer.AddHandler(work)
	err = consumer.ConnectToNSQLookupd(m.consumeAddr)
	if err != nil {
		return err
	}
	return nil
}

func (m *manage) PublishWork(work Work) {
	m.workChan.In <- work
}

func (m *manage) StartRegisterServer(channel string, fn WorkActionFunc) {
	work, b := m.Work(m.registerName)
	if b {
		return
	}
	work = NewConsumeWork(m.registerName, channel, fn)
	m.consumeWorker(work)
}

func (m *manage) ConsumeWork(work Work) {
	m.RegistryWorker(work)
	go m.consumeWorker(work)
}

func (m *manage) StartRegisterClient(channel string, message WorkMessage, fn WorkActionFunc) {
	work := NewPublishWork(DefaultRegisterName, message)
	m.PublishWork(work)

	work = NewConsumeWork(message.Topic, channel, fn)
	m.ConsumeWork(work)
}

func (m *manage) produceWorker() error {
	producer, err := nsq.NewProducer(m.producerAddr, m.nsqConfig)
	if err != nil {
		return err
	}
	defer producer.Stop()
	var work Work
	for {
		errPing := producer.Ping()
		if errPing != nil {
			break
		}
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case work = <-m.workChan.Out:
			err = producer.Publish(work.Topic(), work.Data())
			if err != nil {
				continue
			}
		}
	}
	return nil
}

func initManage(ctx context.Context, registerName string) *manage {
	if registerName == "" {
		registerName = DefaultRegisterName
	}
	return &manage{
		ctx:          ctx,
		registerName: DefaultRegisterName,
		nsqConfig:    nsq.NewConfig(),
		producerAddr: "",
		workChan:     NewWorkChan(5),
	}
}

func NewManager(ctx context.Context, register string) Manager {
	return initManage(ctx, register)
}

type Manager interface {
	NsqConfig() *nsq.Config
	SetNsqConfig(nsqConfig *nsq.Config)
	RegistryWorker(work Work) Work
	Work(topic string) (Work, bool)
	DestroyWork(work Work)
	Works() []Work
	PublishWork(work Work)
	StartRegisterServer(channel string, fn WorkActionFunc)
	ConsumeWork(work Work)
	StartRegisterClient(channel string, message WorkMessage, fn WorkActionFunc)
}
