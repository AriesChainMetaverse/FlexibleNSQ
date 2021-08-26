package fnsq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

var ErrWorkClosed = errors.New("work is closed")
var ErrInputMessageTimeout = errors.New("handle input message timeout")

type WorkActionFunc = func(msg *nsq.Message) error

type Worker interface {
	Consumer(config *nsq.Config, addr string, interval time.Duration, security bool) error
	NewPublisher(message []byte) Publisher
	Topic() string
	Channel() string
	Message() <-chan *nsq.Message
	Closed() bool
	Stop()
}

type work struct {
	once    sync.Once
	closed  chan bool
	message chan *nsq.Message
	topic   string
	channel string
}

func (w *work) Closed() bool {
	select {
	case v, b := <-w.closed:
		if b {
			return v
		}
		return true
	default:
	}
	return false
}

func (w *work) Message() <-chan *nsq.Message {
	return w.message
}

func (w *work) Consumer(config *nsq.Config, addr string, interval time.Duration, security bool) error {
	return w.connect(config, addr, interval, security)
}

func (w *work) connect(config *nsq.Config, addr string, interval time.Duration, security bool) error {
	if w.Closed() {
		return ErrWorkClosed
	}
	consumer, err := nsq.NewConsumer(w.Topic(), w.Channel(), config)
	if err != nil {
		return err
	}
	defer consumer.Stop()
	consumer.AddHandler(w)
	t := time.NewTimer(interval * time.Second)
	defer t.Stop()
	for {
		select {
		case _, _ = <-w.closed:
			if DEBUG {
				fmt.Println("disconnect from:", addr)
			}
			if security {
				if DEBUG {
					//fmt.Println("ConnectToNSQD")
				}
				consumer.IsStarved()
				err = consumer.DisconnectFromNSQD(addr)
			} else {
				if DEBUG {
					//fmt.Println("ConnectToNSQLookupd")
				}
				err = consumer.DisconnectFromNSQLookupd(addr)
				if err != nil {

				}
			}
		case <-t.C:
			if security {
				if DEBUG {
					//fmt.Println("ConnectToNSQD")
				}
				err = consumer.ConnectToNSQD(addr)
			} else {
				if DEBUG {
					//fmt.Println("ConnectToNSQLookupd")
				}
				err = consumer.ConnectToNSQLookupd(addr)
				if err != nil {

				}
			}
			t.Reset(interval * time.Second)
		}
	}
}

func (w *work) Stop() {
	w.once.Do(func() {
		w.closed <- true
		close(w.closed)
		close(w.message)
	})

}

func NewWorker(topic string, channel string) Worker {
	return &work{
		closed:  make(chan bool, 1),
		topic:   topic,
		message: make(chan *nsq.Message, 1024),
		channel: channel,
	}
}

func (w *work) NewPublisher(message []byte) Publisher {
	return &publisher{
		topic:   w.topic,
		message: message,
	}
}

func (w *work) Topic() string {
	return w.topic
}

func (w *work) Channel() string {
	return w.channel
}

func (w *work) HandleMessage(msg *nsq.Message) error {
	if w.Closed() {
		return ErrWorkClosed
	}
	if string(msg.Body) == HelloWorld {
		if DEBUG {
			fmt.Println("received system message:", HelloWorld)
		}
		return nil
	}
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()
	select {
	case w.message <- msg:
	case <-t.C:
		return ErrInputMessageTimeout
	}
	return nil
}
