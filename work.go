package fnsq

import (
	"errors"
	"time"

	"github.com/nsqio/go-nsq"
)

type WorkActionFunc = func(msg *nsq.Message) error

type Worker interface {
	Consumer(config *nsq.Config) (*nsq.Consumer, error)
	Topic() string
	Channel() string
	HandleMessage(msg *nsq.Message) error
	Message() <-chan *nsq.Message
	Data() []byte
	Stop()
}

type work struct {
	consumer *nsq.Consumer
	message  chan *nsq.Message
	topic    string
	channel  string
	data     []byte
}

func (w *work) Message() <-chan *nsq.Message {
	return w.message
}

func (w *work) Consumer(config *nsq.Config) (*nsq.Consumer, error) {
	if w.consumer != nil {
		return w.consumer, nil
	}
	var err error
	w.consumer, err = nsq.NewConsumer(w.Topic(), w.Channel(), config)
	if err != nil {
		return nil, err
	}
	w.consumer.AddHandler(w)
	return w.consumer, nil
}

func (w *work) Stop() {
	if w.consumer != nil {
		w.consumer.Stop()
		w.consumer = nil
	}
}

func NewPublishWorker(topic string, message WorkMessage) Worker {
	return &work{
		topic:   topic,
		message: make(chan *nsq.Message, 1024),
		data:    message.JSON(),
	}
}

func NewConsumeWorker(topic string, channel string) Worker {
	return &work{
		topic:   topic,
		message: make(chan *nsq.Message, 1024),
		channel: channel,
	}
}

func (w *work) SetData(data []byte) {
	w.data = data
}

func (w work) Data() []byte {
	return w.data
}

func (w work) Topic() string {
	return w.topic
}

func (w work) Channel() string {
	return w.channel
}

func (w *work) SetTopic(topic string) {
	w.topic = topic
}

func (w *work) SetChannel(channel string) {
	w.channel = channel
}

func (w work) HandleMessage(msg *nsq.Message) error {
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()
	select {
	case w.message <- msg:
	case <-t.C:
		return errors.New("input time out")
	}
	return nil
}
