package fnsq

import (
	"encoding/json"
)

type ActionCallback = func(data []byte)

type WorkMessage struct {
	Topic  string `json:"topic,omitempty"`
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

func ParseMessage(data []byte) (WorkMessage, error) {
	var in WorkMessage
	err := json.Unmarshal(data, &in)
	return in, err
}
