package amqp

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestMethodFrames_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		method AMQPMethod
	}{
		{
			name: "connection-start",
			method: ConnStartRequest{
				VersionMajor:     0,
				VersionMinor:     9,
				ServerProperties: Table{"product": "erionn-mq"},
				Mechanisms:       "PLAIN",
				Locales:          "en_US",
			},
		},
		{
			name: "connection-start-ok",
			method: ConnStartResponse{
				ClientProperties: Table{"capabilities": Table{"basic.nack": false}},
				Mechanism:        "PLAIN",
				Response:         []byte("\x00guest\x00guest"),
				Locale:           "en_US",
			},
		},
		{
			name: "queue-declare-ok",
			method: QueueDeclareResponse{
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
		{
			name:   "basic-reject",
			method: BasicReject{DeliveryTag: 42, Requeue: true},
		},
		{
			name:   "basic-nack",
			method: BasicNack{DeliveryTag: 77, Multiple: true, Requeue: true},
		},
		{
			name:   "confirm-select",
			method: ConfirmSelectRequest{NoWait: true},
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

func TestDecodeMethodFrame_UnsupportedMethod(t *testing.T) {
	frame := Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{
			0x00, byte(classBasic),
			0x00, 0x7f,
		},
	}

	_, err := DecodeMethodFrame(frame)
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != "amqp: unsupported method 60.127" {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestMethodDecoders_ExactCoverage(t *testing.T) {
	expectedKeys := []methodKey{
		createAMQPMethodKey(classConnection, 10),
		createAMQPMethodKey(classConnection, 11),
		createAMQPMethodKey(classConnection, 30),
		createAMQPMethodKey(classConnection, 31),
		createAMQPMethodKey(classConnection, 40),
		createAMQPMethodKey(classConnection, 41),
		createAMQPMethodKey(classConnection, 50),
		createAMQPMethodKey(classConnection, 51),
		createAMQPMethodKey(classChannel, 10),
		createAMQPMethodKey(classChannel, 11),
		createAMQPMethodKey(classChannel, 40),
		createAMQPMethodKey(classChannel, 41),
		createAMQPMethodKey(classExchange, 10),
		createAMQPMethodKey(classExchange, 11),
		createAMQPMethodKey(classQueue, 10),
		createAMQPMethodKey(classQueue, 11),
		createAMQPMethodKey(classQueue, 20),
		createAMQPMethodKey(classQueue, 21),
		createAMQPMethodKey(classBasic, 10),
		createAMQPMethodKey(classBasic, 11),
		createAMQPMethodKey(classBasic, 20),
		createAMQPMethodKey(classBasic, 21),
		createAMQPMethodKey(classBasic, 30),
		createAMQPMethodKey(classBasic, 31),
		createAMQPMethodKey(classBasic, 40),
		createAMQPMethodKey(classBasic, 60),
		createAMQPMethodKey(classBasic, 80),
		createAMQPMethodKey(classBasic, 90),
		createAMQPMethodKey(classBasic, 120),
		createAMQPMethodKey(classConfirm, 10),
		createAMQPMethodKey(classConfirm, 11),
	}

	gotKeys := make(map[methodKey]struct{}, len(methodDecoders))
	for key, dec := range methodDecoders {
		if dec == nil {
			t.Fatalf("decoder for key %v is nil", key)
		}
		gotKeys[key] = struct{}{}
	}

	if len(gotKeys) != len(expectedKeys) {
		t.Fatalf("decoder count mismatch: got=%d want=%d", len(gotKeys), len(expectedKeys))
	}

	for _, key := range expectedKeys {
		if _, ok := gotKeys[key]; !ok {
			t.Fatalf("missing decoder for key %v", key)
		}
		delete(gotKeys, key)
	}

	for key := range gotKeys {
		t.Fatalf("unexpected decoder for key %v", key)
	}
}

func TestMarshalClose_DecodeClose_RoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		replyCode uint16
		replyText string
		classRef  uint16
		methodRef uint16
	}{
		{
			name:      "connection-close",
			replyCode: 200,
			replyText: "bye",
			classRef:  classConnection,
			methodRef: 50,
		},
		{
			name:      "channel-close",
			replyCode: 404,
			replyText: "not found",
			classRef:  classChannel,
			methodRef: 40,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := marshalClose(&buf, tc.replyCode, tc.replyText, tc.classRef, tc.methodRef); err != nil {
				t.Fatalf("marshalClose: %v", err)
			}

			replyCode, replyText, classRef, methodRef, err := decodeClose(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("decodeClose: %v", err)
			}

			if replyCode != tc.replyCode || replyText != tc.replyText || classRef != tc.classRef || methodRef != tc.methodRef {
				t.Fatalf("decoded mismatch: got=(%d,%q,%d,%d) want=(%d,%q,%d,%d)",
					replyCode, replyText, classRef, methodRef,
					tc.replyCode, tc.replyText, tc.classRef, tc.methodRef)
			}
		})
	}
}

func TestMarshalTune_DecodeTune_RoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		channelMax uint16
		frameMax   uint32
		heartbeat  uint16
	}{
		{
			name:       "connection-tune",
			channelMax: 64,
			frameMax:   131072,
			heartbeat:  30,
		},
		{
			name:       "connection-tune-ok",
			channelMax: 0,
			frameMax:   65536,
			heartbeat:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := marshalTune(&buf, tc.channelMax, tc.frameMax, tc.heartbeat); err != nil {
				t.Fatalf("marshalTune: %v", err)
			}

			channelMax, frameMax, heartbeat, err := decodeTune(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("decodeTune: %v", err)
			}

			if channelMax != tc.channelMax || frameMax != tc.frameMax || heartbeat != tc.heartbeat {
				t.Fatalf("decoded mismatch: got=(%d,%d,%d) want=(%d,%d,%d)",
					channelMax, frameMax, heartbeat,
					tc.channelMax, tc.frameMax, tc.heartbeat)
			}
		})
	}
}

func TestDecodeClose_PropagatesShortRead(t *testing.T) {
	_, _, _, _, err := decodeClose(bytes.NewReader([]byte{0x00}))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected error: %v", err)
	}
}
