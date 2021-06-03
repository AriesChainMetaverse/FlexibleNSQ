package fnsq

import (
	"encoding/json"
)

type ActionCallback = func(data []byte)

type WorkMessage struct {
	ID     string `json:"id,omitempty"`
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

func NewWorkMessage(id string, topic string, last int64, data []byte) WorkMessage {
	return WorkMessage{
		ID:     id,
		Topic:  topic,
		Last:   last,
		Length: len(data),
		Data:   data,
	}
}

func ParseMessage(data []byte) (WorkMessage, error) {
	var in WorkMessage
	err := json.Unmarshal(data, &in)
	return in, err
}

func (m WorkMessage) Work(data []byte, last int64) Worker {
	return NewPublishWorker(m.Topic, WorkMessage{
		Topic:  m.Topic,
		Last:   last,
		Length: len(data),
		Data:   data,
	})
}
