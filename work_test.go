package fnsq

import (
	"reflect"
	"testing"
)

func TestNewWorker(t *testing.T) {
	type args struct {
		topic   string
		channel string
	}
	tests := []struct {
		name string
		args args
		want Worker
	}{
		{
			name: "",
			args: args{
				topic:   "topic1",
				channel: "channel1",
			},
			want: NewWorker("topic1", "channel1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewWorker(tt.args.topic, tt.args.channel); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}
