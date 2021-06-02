package fnsq

import (
	"github.com/nsqio/go-nsq"
)

type WorkActionFunc = func(msg *nsq.Message) error

type Work interface {
	Topic() string
	Channel() string
	HandleMessage(msg *nsq.Message) error
	Data() []byte
}

type work struct {
	actionFunc WorkActionFunc
	topic      string
	channel    string
	data       []byte
}

func NewPublishWork(topic string, message WorkMessage) Work {
	return &work{
		topic: topic,
		data:  message.JSON(),
	}
}

func NewConsumeWork(topic string, channel string, fn WorkActionFunc) Work {
	return &work{
		topic:      topic,
		channel:    channel,
		actionFunc: fn,
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

func (w *work) SetActionFunc(actionFunc WorkActionFunc) {
	w.actionFunc = actionFunc
}

func (w *work) SetTopic(topic string) {
	w.topic = topic
}

func (w *work) SetChannel(channel string) {
	w.channel = channel
}

func (w work) HandleMessage(msg *nsq.Message) error {
	if w.actionFunc != nil {
		return w.actionFunc(msg)
	}
	return nil
}
