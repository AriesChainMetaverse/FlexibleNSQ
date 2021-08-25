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
	PublishWorker(work Worker)
	RegisterServer(channel string) Worker
	RegisterClient(topic, channel string, message []byte) Worker
	ConsumeWorker(work Worker, delay int)
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
	workChan   *WorkerChan
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

func (m *manage) consumeWorker(work Worker) error {
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

func (m *manage) PublishWorker(work Worker) {
	m.workChan.In <- work
}

func (m *manage) RegisterServer(channel string) Worker {
	m.PublishWorker(NewPublishWorker(m.config.RegisterName, []byte(HelloWorld)))
	return m.RegisterConsumeWorker(m.config.RegisterName, channel, 0)
}

func (m *manage) RegisterConsumeWorker(topic string, channel string, delay int) Worker {
	work, b := m.registryWorker(NewConsumeWorker(topic, channel))
	if b {
		return work
	}
	m.ConsumeWorker(work, delay)
	return work
}

func (m *manage) RegisterClient(topic, channel string, message []byte) Worker {
	m.PublishWorker(NewPublishWorker(m.config.RegisterName, message))
	m.PublishWorker(NewPublishWorker(topic, []byte(HelloWorld)))
	return m.RegisterConsumeWorker(topic, channel, 5)
}

func (m *manage) ConsumeWorker(work Worker, delay int) {
	go func(delay int) {
		if delay != 0 {
			t := time.NewTimer(time.Duration(delay) * time.Second)
			defer t.Stop()
			select {
			case <-t.C:
				m.consumeWorker(work)
			}
		} else {
			m.consumeWorker(work)
		}
	}(delay)
}

func (m *manage) Start() {
	go m.produceWorker()
}

func (m *manage) Stop() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	for _, w := range m.Workers() {
		w.Stop()
	}
}

func (m *manage) Wait() {
	<-m.ctx.Done()
}

func (m *manage) produceWorker() error {
	producer, err := nsq.NewProducer(m.config.ProducerAddr, m.nsqConfig)
	if err != nil {
		return err
	}
	defer producer.Stop()
	var work Worker
	for {
		errPing := producer.Ping()
		if errPing != nil {
			return err
		}
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case work = <-m.workChan.Out:
			if DEBUG {
				fmt.Println("work data:", "topic", work.Topic(), "data", work.Data())
			}
			err = producer.Publish(work.Topic(), work.Data())
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

func initManage(ctx context.Context, config Config) Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &manage{
		ctx:       ctx,
		cancel:    cancel,
		config:    config,
		nsqConfig: nsq.NewConfig(),
		workers:   make(map[string]Worker, 1),
		workChan:  NewWorkChan(5),
	}
}

func NewManager(ctx context.Context, config Config) Manager {
	return initManage(ctx, config)
}
