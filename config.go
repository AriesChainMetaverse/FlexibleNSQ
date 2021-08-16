package fnsq

import (
	"time"
)

const DefaultRegisterName = "register"

type Config struct {
	RegisterName     string
	ProducerAddr     string //"127.0.0.1:4150"
	ConsumeAddr      string //"127.0.0.1:4160"
	IgnoreReceiveErr bool
	UseSecurity      bool
	Interval         time.Duration
}

func DefaultConfig() Config {
	return Config{
		RegisterName:     DefaultRegisterName,
		ProducerAddr:     "127.0.0.1:4150",
		ConsumeAddr:      "127.0.0.1:4160",
		Interval:         3,
		IgnoreReceiveErr: true,
	}
}
