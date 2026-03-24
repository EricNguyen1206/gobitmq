package amqp

import (
	"bytes"
	"reflect"
	"testing"
)

func TestProtocolHeader_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteProtocolHeader(&buf); err != nil {
		t.Fatalf("WriteProtocolHeader: %v", err)
	}
	if err := ReadProtocolHeader(&buf); err != nil {
		t.Fatalf("ReadProtocolHeader: %v", err)
	}
}

func TestFrame_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	frame := Frame{Type: FrameMethod, Channel: 7, Payload: []byte("hello")}
	if err := WriteFrame(&buf, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if got.Type != frame.Type || got.Channel != frame.Channel || !bytes.Equal(got.Payload, frame.Payload) {
		t.Fatalf("round trip mismatch: got=%+v want=%+v", got, frame)
	}
}

func TestContentHeader_RoundTrip(t *testing.T) {
	head := ContentHeader{
		ClassID:  classBasic,
		BodySize: 5,
		Properties: BasicProperties{
			ContentType:   "text/plain",
			CorrelationID: "cid-1",
			ReplyTo:       "reply-queue",
			DeliveryMode:  2,
			Headers: Table{
				"bool":   true,
				"nested": Table{"key": "value"},
				"number": int32(42),
			},
		},
	}

	frame, err := EncodeContentHeaderFrame(1, head)
	if err != nil {
		t.Fatalf("EncodeContentHeaderFrame: %v", err)
	}

	got, err := DecodeContentHeaderFrame(frame)
	if err != nil {
		t.Fatalf("DecodeContentHeaderFrame: %v", err)
	}

	if got.ClassID != head.ClassID || got.BodySize != head.BodySize {
		t.Fatalf("header mismatch: got=%+v want=%+v", got, head)
	}
	if !reflect.DeepEqual(got.Properties, head.Properties) {
		t.Fatalf("properties mismatch: got=%#v want=%#v", got.Properties, head.Properties)
	}
}
