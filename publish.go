package fnsq

type Publisher interface {
	Topic() string
	Message() []byte
}

type publisher struct {
	topic   string
	message []byte
}

func (p publisher) Topic() string {
	return p.topic
}

func (p publisher) Message() []byte {
	return p.message
}

var _ Publisher = (*publisher)(nil)
