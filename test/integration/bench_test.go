package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func startBrokerForBench(b *testing.B) *broker {
	b.Helper()

	root := benchProjectRoot(b)
	tempDir := b.TempDir()

	binary := filepath.Join(tempDir, "erionn-mq")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	build := exec.Command("go", "build", "-o", binary, "./cmd")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		b.Fatalf("go build: %v\n%s", err, out)
	}

	amqpPort := freeBenchPort(b)
	mgmtPort := freeBenchPort(b)

	cmd := exec.Command(binary)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("ERIONN_AMQP_ADDR=127.0.0.1:%d", amqpPort),
		fmt.Sprintf("ERIONN_MGMT_ADDR=127.0.0.1:%d", mgmtPort),
		fmt.Sprintf("ERIONN_DATA_DIR=%s", filepath.Join(tempDir, "data")),
		"ERIONN_MGMT_USERS=guest:guest:admin",
		"ERIONN_MGMT_ALLOW_REMOTE=true",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		b.Fatalf("start broker: %v", err)
	}

	bk := &broker{
		cmd:     cmd,
		tempDir: tempDir,
		amqpURL: fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", amqpPort),
		mgmtURL: fmt.Sprintf("http://127.0.0.1:%d", mgmtPort),
	}

	waitForBenchPort(b, amqpPort)
	waitForBenchPort(b, mgmtPort)

	b.Cleanup(func() {
		if bk.cmd.Process != nil {
			_ = bk.cmd.Process.Kill()
			_ = bk.cmd.Wait()
		}
	})

	return bk
}

func BenchmarkPublish(b *testing.B) {
	bk := startBrokerForBench(b)

	conn, err := amqp.Dial(bk.amqpURL)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel: %v", err)
	}
	defer ch.Close()

	queue := fmt.Sprintf("bench.publish.%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		b.Fatalf("QueueDeclare: %v", err)
	}

	ctx := context.Background()
	body := []byte("benchmark-payload-64-bytes-padded-to-fill-some-reasonable-space!")

	b.ResetTimer()
	b.SetBytes(int64(len(body)))

	for b.Loop() {
		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: body}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func BenchmarkPublishConsume(b *testing.B) {
	bk := startBrokerForBench(b)

	conn, err := amqp.Dial(bk.amqpURL)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel: %v", err)
	}
	defer ch.Close()

	queue := fmt.Sprintf("bench.pubcon.%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		b.Fatalf("QueueDeclare: %v", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume: %v", err)
	}

	ctx := context.Background()
	body := []byte("benchmark-payload-64-bytes-padded-to-fill-some-reasonable-space!")

	b.ResetTimer()
	b.SetBytes(int64(len(body)))

	for b.Loop() {
		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: body}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		msg := <-msgs
		if err := msg.Ack(false); err != nil {
			b.Fatalf("Ack: %v", err)
		}
	}
}

func BenchmarkLatency(b *testing.B) {
	bk := startBrokerForBench(b)

	conn, err := amqp.Dial(bk.amqpURL)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel: %v", err)
	}
	defer ch.Close()

	queue := fmt.Sprintf("bench.latency.%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		b.Fatalf("QueueDeclare: %v", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume: %v", err)
	}

	ctx := context.Background()
	body := []byte("latency")

	b.ResetTimer()

	for b.Loop() {
		start := time.Now()
		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: body}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		msg := <-msgs
		if err := msg.Ack(false); err != nil {
			b.Fatalf("Ack: %v", err)
		}
		b.ReportMetric(float64(time.Since(start).Microseconds()), "µs/op")
	}
}

func benchProjectRoot(b *testing.B) string {
	b.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		b.Fatal("cannot determine project root")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func freeBenchPort(b *testing.B) int {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("allocate port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func waitForBenchPort(b *testing.B, port int) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.Fatalf("timeout waiting for port %d", port)
}
