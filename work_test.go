package fnsq

import (
	"context"
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
			want: NewWorker(context.TODO(), "topic1", "channel1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewWorker(context.TODO(), tt.args.topic, tt.args.channel)
			if got.Topic() != tt.want.Topic() {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
			if got.Channel() != tt.want.Channel() {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewWorker_Closed(t *testing.T) {
	type args struct {
		topic   string
		channel string
	}
	tests := []struct {
		name   string
		args   args
		action func(worker Worker)
		want   bool
	}{
		{
			name: "",
			args: args{
				topic:   "topic1",
				channel: "channel1",
			},
			want: false,
		},
		{
			name: "",
			args: args{
				topic:   "topic1",
				channel: "channel1",
			},
			action: func(worker Worker) {
				worker.Destroy()
			},
			want: true,
		},
		{
			name: "",
			args: args{
				topic:   "topic1",
				channel: "channel1",
			},
			action: func(worker Worker) {
				worker.Destroy()
				worker.Destroy()
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewWorker(context.Background(), tt.args.topic, tt.args.channel)
			if tt.action != nil {
				tt.action(got)
			}
			if got.Closed() != tt.want {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
			if got.Closed() != tt.want {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}
