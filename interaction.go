package fnsq

import (
	"encoding/json"
)

type ActionCallback = func(data []byte)

type Interaction struct {
	Topic   string `json:"topic"`
	Channel string `json:"channel"`
	Data    []byte `json:"data"`
}

func ParseInteraction(data string) (Interaction, error) {
	var in Interaction
	err := json.Unmarshal([]byte(data), &in)
	return in, err
}
