package fnsq

import (
	"github.com/nsqio/go-nsq"
)

type Work interface {
	Topic() string
	Channel() string
	Call(msg *nsq.Message)
}

type work struct {
	in Interaction
}

func (w work) Topic() string {
	return w.in.Topic
}

func (w work) Channel() string {
	return w.in.Channel
}

func NewWork(interaction Interaction) Work {
	return &work{
		in: interaction,
	}
}

func (w work) Call(msg *nsq.Message) {

}
