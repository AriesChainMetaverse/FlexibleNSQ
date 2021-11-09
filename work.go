package fnsq

import (
	"context"
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
	Context() context.Context
	Closed() bool
	Destroy()
	HookDestroy(fn func(worker Worker))
}

type work struct {
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
	message   chan *nsq.Message
	topic     string
	channel   string
	onDestroy func(worker Worker)
}

func (w *work) Context() context.Context {
	return w.ctx
}

func (w *work) HookDestroy(fn func(worker Worker)) {
	w.onDestroy = fn
}

func (w *work) Closed() bool {
	select {
	case <-w.ctx.Done():
		return true
	default:
		return false
	}
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
	defer w.Destroy()
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
		case <-w.ctx.Done():
			if DEBUG {
				fmt.Println("disconnect from:", addr)
			}
			return nil
		case <-t.C:
			if security {
				err = consumer.ConnectToNSQD(addr)
			} else {
				err = consumer.ConnectToNSQLookupd(addr)
			}
			if err != nil {
				return err
			}
			t.Reset(interval * time.Second)
		}
	}
}

func (w *work) Destroy() {
	w.once.Do(func() {
		if w.cancel != nil {
			w.cancel()
			w.cancel = nil
		}
		close(w.message)
		if w.onDestroy != nil {
			w.onDestroy(w)
			w.onDestroy = nil
		}
	})

}

func NewWorker(ctx context.Context, topic string, channel string) Worker {
	ctx, cancel := context.WithCancel(ctx)
	return &work{
		ctx:     ctx,
		cancel:  cancel,
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
