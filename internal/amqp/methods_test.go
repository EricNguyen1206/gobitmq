package amqp

import (
	"reflect"
	"testing"
)

func TestMethodFrames_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		method Method
	}{
		{
			name: "connection-start",
			method: ConnectionStart{
				VersionMajor:     0,
				VersionMinor:     9,
				ServerProperties: Table{"product": "erionn-mq"},
				Mechanisms:       "PLAIN",
				Locales:          "en_US",
			},
		},
		{
			name: "connection-start-ok",
			method: ConnectionStartOk{
				ClientProperties: Table{"capabilities": Table{"basic.nack": false}},
				Mechanism:        "PLAIN",
				Response:         []byte("\x00guest\x00guest"),
				Locale:           "en_US",
			},
		},
		{
			name: "queue-declare-ok",
			method: QueueDeclareOk{
				Queue:         "jobs",
				MessageCount:  3,
				ConsumerCount: 1,
			},
		},
		{
			name: "basic-publish",
			method: BasicPublish{
				Exchange:   "events",
				RoutingKey: "user.created",
				Mandatory:  true,
			},
		},
		{
			name: "basic-deliver",
			method: BasicDeliver{
				ConsumerTag: "ctag-1",
				DeliveryTag: 99,
				Redelivered: true,
				Exchange:    "events",
				RoutingKey:  "user.created",
			},
		},
		{
			name:   "basic-ack",
			method: BasicAck{DeliveryTag: 99, Multiple: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := EncodeMethodFrame(1, tc.method)
			if err != nil {
				t.Fatalf("EncodeMethodFrame: %v", err)
			}

			got, err := DecodeMethodFrame(frame)
			if err != nil {
				t.Fatalf("DecodeMethodFrame: %v", err)
			}

			if !reflect.DeepEqual(got, tc.method) {
				t.Fatalf("decoded method mismatch: got=%#v want=%#v", got, tc.method)
			}
		})
	}
}
