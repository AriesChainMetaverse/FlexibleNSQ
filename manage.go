package fnsq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type Manager interface {
	RegisterName() string
	NSQConfig() *nsq.Config
	SetNSQConfig(nsqConfig *nsq.Config)
	Worker(topic string) (Worker, bool)
	DestroyWorker(topic string) bool
	Workers() []Worker
	Server() Server
	PublishMessage(topic string, message []byte)
	Publisher(pub Publisher)
	RegisterWorker(topic, channel string) Worker
	Start()
	Stop()
	Wait()
}

type manage struct {
	ctx        context.Context
	cancel     context.CancelFunc
	config     Config
	nsqConfig  *nsq.Config
	workerLock sync.RWMutex
	workers    map[string]Worker
	msgChan    *MessageChan
}

func (m *manage) NSQConfig() *nsq.Config {
	return m.nsqConfig
}

func (m *manage) SetNSQConfig(nsqConfig *nsq.Config) {
	m.nsqConfig = nsqConfig
}

func (m *manage) addWorker(worker Worker) {
	m.workerLock.Lock()
	m.workers[worker.Topic()] = worker
	m.workerLock.Unlock()
}

func (m *manage) registryWorker(work Worker) (Worker, bool) {
	worker, b := m.Worker(work.Topic())
	if b {
		return worker, true
	}
	m.addWorker(work)
	return work, false
}

func (m *manage) RegisterName() string {
	return m.config.RegisterName
}

func (m *manage) Worker(topic string) (Worker, bool) {
	m.workerLock.RLock()
	work, exist := m.workers[topic]
	m.workerLock.RUnlock()
	return work, exist
}

func (m *manage) DestroyWorker(topic string) bool {
	workers, exist := m.Worker(topic)
	if !exist {
		return false
	}
	m.workerLock.Lock()
	delete(m.workers, topic)
	m.workerLock.Unlock()
	workers.Stop()
	return true
}

func (m *manage) Workers() []Worker {
	var works []Worker
	m.workerLock.Lock()
	for i := range m.workers {
		works = append(works, m.workers[i])
	}
	m.workerLock.Unlock()
	return works
}

func (m *manage) consumeProcessor(work Worker) error {
	consumer, err := work.Consumer(m.nsqConfig)
	if err != nil {
		return err
	}
	t := time.NewTimer(m.config.Interval * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
		case <-t.C:
			if m.config.UseSecurity {
				if DEBUG {
					//fmt.Println("ConnectToNSQD")
				}
				err = consumer.ConnectToNSQD(m.config.ConsumeAddr)
			} else {
				if DEBUG {
					//fmt.Println("ConnectToNSQLookupd")
				}
				err = consumer.ConnectToNSQLookupd(m.config.ConsumeAddr)
				if err != nil {

				}
			}
			t.Reset(m.config.Interval * time.Second)
		}
	}
}

func (m *manage) PublishMessage(topic string, message []byte) {
	m.msgChan.In <- &publisher{
		topic:   topic,
		message: message,
	}
}

func (m *manage) Publisher(pub Publisher) {
	m.msgChan.In <- pub
}

func (m *manage) registerConsumeWorker(topic string, channel string, delay int) Worker {
	_work := NewWorker(topic, channel)
	m.addWorker(_work)
	m.consumeWorker(_work, delay)
	return _work
}

func (m *manage) PublishRegisterMessage(message []byte) {
	m.PublishMessage(m.config.RegisterName, message)
}

func (m *manage) RegisterWorker(topic, channel string) Worker {
	_work, b := m.Worker(topic)
	if b {
		return _work
	}
	return m.register(topic, channel)
}

func (m *manage) register(topic, channel string) Worker {
	m.PublishMessage(topic, []byte(HelloWorld))
	return m.registerConsumeWorker(topic, channel, 0)
}

func (m *manage) consumeWorker(work Worker, delay int) {
	go func(delay int) {
		if delay != 0 {
			t := time.NewTimer(time.Duration(delay) * time.Second)
			defer t.Stop()
			select {
			case <-t.C:
				m.consumeProcessor(work)
			}
		} else {
			m.consumeProcessor(work)
		}
	}(delay)
}

func (m *manage) Start() {
	//m.register(topic, channel)
	m.start()
}

func (m *manage) start() {
	go m.publishProducer()
}

func (m *manage) Stop() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	for _, w := range m.Workers() {
		m.DestroyWorker(w.Topic())
	}
}

func (m *manage) Wait() {
	<-m.ctx.Done()
}

func (m *manage) publishProducer() error {
	producer, err := nsq.NewProducer(m.config.ProducerAddr, m.nsqConfig)
	if err != nil {
		return err
	}
	defer producer.Stop()
	var _message Publisher
	for {
		errPing := producer.Ping()
		if errPing != nil {
			return err
		}
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case _message = <-m.msgChan.Out:
			if DEBUG {
				fmt.Println("_message data:", "topic", _message.Topic(), "message", _message.Message())
			}
			err = producer.Publish(_message.Topic(), _message.Message())
			if err != nil {
				fmt.Println("ERR:", err)
				if !m.config.IgnoreReceiveErr {
					return err
				}
			}

		}
	}
	return nil
}

func (m *manage) Server() Server {
	return (*manageServer)(m)
}

func initManage(ctx context.Context, config Config) Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &manage{
		ctx:       ctx,
		cancel:    cancel,
		config:    config,
		nsqConfig: nsq.NewConfig(),
		workers:   make(map[string]Worker, 1),
		msgChan:   NewWorkChan(5),
	}
}

func NewManager(ctx context.Context, config Config) Manager {
	return initManage(ctx, config)
}
