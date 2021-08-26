package fnsq

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/DragonveinChain/FlexibleNSQ/message"
)

const HelloWorld = "#hello#world#"

type ActionCallback = func(data []byte)

type WorkMessage message.Message

//type WorkMessage struct {
//	ID     string `json:"id,omitempty"`
//	Topic  string `json:"topic,omitempty"`
//	Last   int64  `json:"last,omitempty"`
//	Length int    `json:"length,omitempty"`
//	Data   []byte `json:"data,omitempty"`
//}
//
//func (m WorkMessage) JSON() []byte {
//	msg, err := json.Marshal(m)
//	if err != nil {
//		return nil
//	}
//	return msg
//}
//
//func (m WorkMessage) String() string {
//	return string(m.JSON())
//}

func NewMessageData(id string, topic string, last int64, data []byte) []byte {
	builder := flatbuffers.NewBuilder(1024)
	_id := builder.CreateString(id)
	_topic := builder.CreateString(topic)
	_data := builder.CreateByteString(data)

	message.MessageStart(builder)
	message.MessageAddId(builder, _id)
	message.MessageAddTopic(builder, _topic)
	message.MessageAddLast(builder, last)
	message.MessageAddData(builder, _data)
	builder.Finish(message.MessageEnd(builder))

	return builder.FinishedBytes()
}

func ParseMessage(data []byte) *WorkMessage {
	return (*WorkMessage)(message.GetRootAsMessage(data, 0))
}

func (m *WorkMessage) msg() *message.Message {
	return (*message.Message)(m)
}

func (m *WorkMessage) Topic() string {
	return string(m.msg().Topic())
}

func (m *WorkMessage) ID() string {
	return string(m.msg().Id())
}

func (m *WorkMessage) Last() int64 {
	return m.msg().Last()
}

func (m *WorkMessage) Data() []byte {
	return m.msg().Data()
}

func (m *WorkMessage) NewPublisher(data []byte, last int64) Publisher {
	return &publisher{
		topic:   m.Topic(),
		message: NewMessageData(m.ID(), m.Topic(), last, data),
	}
}
