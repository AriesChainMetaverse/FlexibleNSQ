package fnsq

import (
	"encoding/json"
)

type ActionCallback = func(data []byte)

type WorkMessage struct {
	Topic   string `json:"topic"`
	Channel string `json:"channel"`
	Data    []byte `json:"data"`
}

func ParseMessage(data []byte) (WorkMessage, error) {
	var in WorkMessage
	err := json.Unmarshal(data, &in)
	return in, err
}
