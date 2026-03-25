package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// helpers -----------------------------------------------------------------

type broker struct {
	cmd     *exec.Cmd
	tempDir string
	amqpURL string
	mgmtURL string
}

func startBroker(t *testing.T) *broker {
	t.Helper()
	root := projectRoot(t)
	tempDir := t.TempDir()

	binary := filepath.Join(tempDir, "erionn-mq")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	build := exec.Command("go", "build", "-o", binary, "./cmd")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	amqpPort := freePort(t)
	mgmtPort := freePort(t)

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
		t.Fatalf("start broker: %v", err)
	}

	b := &broker{
		cmd:     cmd,
		tempDir: tempDir,
		amqpURL: fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", amqpPort),
		mgmtURL: fmt.Sprintf("http://127.0.0.1:%d", mgmtPort),
	}

	waitForPort(t, amqpPort)
	waitForPort(t, mgmtPort)
	return b
}

func (b *broker) stop(t *testing.T) {
	t.Helper()
	if b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
		_ = b.cmd.Wait()
	}
}

func (b *broker) restart(t *testing.T) {
	t.Helper()
	b.stop(t)

	binary := filepath.Join(b.tempDir, "erionn-mq")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	cmd := exec.Command(binary)
	cmd.Dir = projectRoot(t)
	cmd.Env = b.cmd.Env
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("restart broker: %v", err)
	}
	b.cmd = cmd

	// Extract ports from URLs to wait on
	amqpPort := portFromURL(t, b.amqpURL)
	mgmtPort := portFromURL(t, b.mgmtURL)
	waitForPort(t, amqpPort)
	waitForPort(t, mgmtPort)
}

func projectRoot(t *testing.T) string {
	t.Helper()
	// test/integration/ -> project root is ../..
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine project root")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func portFromURL(t *testing.T, rawURL string) int {
	t.Helper()
	// Parse port from amqp://...:<port>/ or http://...:<port>
	var port int
	for i := len(rawURL) - 1; i >= 0; i-- {
		if rawURL[i] == ':' {
			fmt.Sscanf(rawURL[i+1:], "%d", &port)
			break
		}
		if rawURL[i] == '/' {
			// strip trailing slash
			rawURL = rawURL[:i]
		}
	}
	if port == 0 {
		t.Fatalf("cannot parse port from %q", rawURL)
	}
	return port
}

func waitForPort(t *testing.T, port int) {
	t.Helper()
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
	t.Fatalf("timeout waiting for port %d", port)
}

func connect(t *testing.T, url string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("amqp.Dial: %v", err)
	}
	return conn
}

func openChannel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	return ch
}

func unique(prefix string) string {
	return fmt.Sprintf("%s.%d", prefix, time.Now().UnixNano())
}

// tests -------------------------------------------------------------------

func TestPublishConsumeAck(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	exchange := unique("go-compat.ex")
	queue := unique("go-compat.q")
	routingKey := unique("go-compat.key")

	if err := ch.ExchangeDeclare(exchange, "direct", false, false, false, false, nil); err != nil {
		t.Fatalf("ExchangeDeclare: %v", err)
	}
	q, err := ch.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	if err := ch.QueueBind(q.Name, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("QueueBind: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("hello from Go"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "hello from Go" {
			t.Fatalf("unexpected body: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestNackAndRequeue(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	queue := unique("go-compat.nack")
	q, err := ch.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test nack without requeue (drop)
	if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{Body: []byte("drop")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "dropper", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "drop" {
			t.Fatalf("unexpected body: %q", msg.Body)
		}
		if err := msg.Nack(false, false); err != nil {
			t.Fatalf("Nack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for drop message")
	}

	if err := ch.Cancel("dropper", false); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	qi, err := ch.QueueInspect(q.Name)
	if err != nil {
		t.Fatalf("QueueInspect: %v", err)
	}
	if qi.Messages != 0 {
		t.Fatalf("expected 0 messages after nack without requeue, got %d", qi.Messages)
	}

	// Test nack with requeue
	if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{Body: []byte("retry")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msgs2, err := ch.Consume(q.Name, "requeuer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// First delivery - nack with requeue
	select {
	case msg := <-msgs2:
		if err := msg.Nack(false, true); err != nil {
			t.Fatalf("Nack requeue: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for first delivery")
	}

	// Second delivery - should be redelivered
	select {
	case msg := <-msgs2:
		if !msg.Redelivered {
			t.Fatal("expected redelivered=true")
		}
		if string(msg.Body) != "retry" {
			t.Fatalf("unexpected body: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for redelivered message")
	}
}

func TestPrefetch(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	queue := unique("go-compat.prefetch")
	q, err := ch.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("Qos: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{Body: []byte("first")}); err != nil {
		t.Fatalf("Publish first: %v", err)
	}
	if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{Body: []byte("second")}); err != nil {
		t.Fatalf("Publish second: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Should get first message
	var firstMsg amqp.Delivery
	select {
	case firstMsg = <-msgs:
		if string(firstMsg.Body) != "first" {
			t.Fatalf("unexpected first body: %q", firstMsg.Body)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for first message")
	}

	// Second should NOT arrive while first is unacked
	select {
	case <-msgs:
		t.Fatal("second message arrived before first was acked (prefetch=1 violation)")
	case <-time.After(300 * time.Millisecond):
		// expected
	}

	// Ack first, then second should arrive
	if err := firstMsg.Ack(false); err != nil {
		t.Fatalf("Ack first: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "second" {
			t.Fatalf("unexpected second body: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack second: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for second message after ack")
	}
}

func TestManagementAPI(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)

	queue := unique("go-compat.mgmt")
	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare: %v", err)
	}
	ch.Close()
	conn.Close()

	// Unauthorized request
	resp, err := http.Get(b.mgmtURL + "/api/overview")
	if err != nil {
		t.Fatalf("GET overview: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// Authorized overview
	req, _ := http.NewRequest("GET", b.mgmtURL+"/api/overview", nil)
	req.SetBasicAuth("guest", "guest")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET overview auth: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var overview map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	stats, ok := overview["message_stats"].(map[string]any)
	if !ok {
		t.Fatal("overview missing message_stats")
	}
	if _, ok := stats["publish"]; !ok {
		t.Fatal("message_stats missing 'publish' field")
	}

	// Authorized queues list
	req2, _ := http.NewRequest("GET", b.mgmtURL+"/api/queues", nil)
	req2.SetBasicAuth("guest", "guest")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET queues: %v", err)
	}
	defer resp2.Body.Close()

	body, _ := io.ReadAll(resp2.Body)
	var queues []map[string]any
	if err := json.Unmarshal(body, &queues); err != nil {
		t.Fatalf("decode queues: %v", err)
	}

	found := false
	for _, q := range queues {
		if q["name"] == queue {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("queue %q not found in management API queue list", queue)
	}
}

func TestDurableRestart(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.durable")

	// Publish a persistent message
	func() {
		conn := connect(t, b.amqpURL)
		defer conn.Close()
		ch := openChannel(t, conn)
		defer ch.Close()

		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte("persisted"),
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}()

	// Restart broker
	b.restart(t)

	// Verify message survived
	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare after restart: %v", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case msg := <-msgs:
		if string(msg.Body) != "persisted" {
			t.Fatalf("unexpected body after restart: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout: durable message did not survive restart")
	}
}

func TestReconnectAfterRestart(t *testing.T) {
	b := startBroker(t)
	defer b.stop(t)

	queue := unique("go-compat.reconnect")

	// Create queue before restart
	func() {
		conn := connect(t, b.amqpURL)
		defer conn.Close()
		ch := openChannel(t, conn)
		defer ch.Close()

		if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			t.Fatalf("QueueDeclare: %v", err)
		}
	}()

	// Restart broker
	b.restart(t)

	// New connection should work
	conn := connect(t, b.amqpURL)
	defer conn.Close()
	ch := openChannel(t, conn)
	defer ch.Close()

	if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare after restart: %v", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{Body: []byte("after-reconnect")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "after-reconnect" {
			t.Fatalf("unexpected body: %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout: new connection failed after restart")
	}
}
