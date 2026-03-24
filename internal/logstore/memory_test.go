package logstore_test

import (
	"testing"

	"erionn-mq/internal/logstore"
	"erionn-mq/internal/partition"
)

func TestMemoryLog_AppendAndLastOffset(t *testing.T) {
	log := logstore.NewMemoryLog()

	if got := log.LastOffset(); got != -1 {
		t.Fatalf("expected initial LastOffset=-1, got %d", got)
	}

	for i := 0; i < 3; i++ {
		offset, err := log.Append(partition.Record{Value: []byte("v")})
		if err != nil {
			t.Fatalf("Append[%d] unexpected error: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("Append[%d] expected offset=%d, got %d", i, i, offset)
		}
	}

	if got := log.LastOffset(); got != 2 {
		t.Fatalf("expected LastOffset=2, got %d", got)
	}
}

func TestMemoryLog_Read_FromOffset(t *testing.T) {
	log := logstore.NewMemoryLog()

	for i := 0; i < 5; i++ {
		log.Append(partition.Record{Key: []byte("k"), Value: []byte{byte(i)}}) //nolint:errcheck
	}

	records, err := log.Read(2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records starting at offset 2, got %d", len(records))
	}
	if records[0].Offset != 2 {
		t.Fatalf("expected first record offset=2, got %d", records[0].Offset)
	}
}

func TestMemoryLog_Read_MaxLimit(t *testing.T) {
	log := logstore.NewMemoryLog()

	for i := 0; i < 10; i++ {
		log.Append(partition.Record{Value: []byte("x")}) //nolint:errcheck
	}

	records, err := log.Read(0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("expected max=4 records, got %d", len(records))
	}
}

func TestMemoryLog_Read_ImmutableCopy(t *testing.T) {
	log := logstore.NewMemoryLog()
	log.Append(partition.Record{Value: []byte("original")}) //nolint:errcheck

	records, _ := log.Read(0, 1)
	records[0].Value[0] = 'X' // mutate the returned slice

	// Re-read: internal state must be unchanged.
	again, _ := log.Read(0, 1)
	if again[0].Value[0] == 'X' {
		t.Fatal("Read returned a reference into internal storage (not a copy)")
	}
}
