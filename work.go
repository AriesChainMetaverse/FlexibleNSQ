package fnsq

import (
	"github.com/nsqio/go-nsq"
)

type WorkActionFunc = func(msg *nsq.Message) error

type Work interface {
	Topic() string
	Channel() string
	HandleMessage(msg *nsq.Message) error
}

type work struct {
	in Interaction
	fn WorkActionFunc
}

func NewWork(interaction Interaction, fn WorkActionFunc) Work {
	return &work{
		in: interaction,
		fn: fn,
	}
}

func (w work) Topic() string {
	return w.in.Topic
}

func (w work) Channel() string {
	return w.in.Channel
}

func (w work) HandleMessage(msg *nsq.Message) error {
	if w.fn != nil {
		return w.fn(msg)
	}
	return nil
}
