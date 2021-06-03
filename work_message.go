package fnsq

import (
	"encoding/json"
)

type ActionCallback = func(data []byte)

type WorkMessage struct {
	Topic  string `json:"topic,omitempty"`
	Last   int64  `json:"last,omitempty"`
	Length int    `json:"length,omitempty"`
	Data   []byte `json:"data,omitempty"`
}

func (m WorkMessage) JSON() []byte {
	msg, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return msg
}

func (m WorkMessage) String() string {
	return string(m.JSON())
}

func ParseMessage(data []byte) (WorkMessage, error) {
	var in WorkMessage
	err := json.Unmarshal(data, &in)
	return in, err
}

func (m WorkMessage) Work(data []byte, last int64) Worker {
	return NewPublishWork(m.Topic, WorkMessage{
		Topic:  m.Topic,
		Last:   last,
		Length: len(data),
		Data:   data,
	})
}
