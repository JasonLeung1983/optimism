package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ethereum/go-ethereum/log"
)

// newTestLogger returns a no-op logger suitable for tests.
func newTestLogger(t *testing.T) log.Logger {
	t.Helper()
	// Root logger with no handlers is effectively silent.
	return log.New()
}

// wsEchoServer starts a simple websocket echo server for tests.
func wsEchoServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "test complete")

		ctx := context.Background()
		for {
			msgType, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if err := conn.Write(ctx, msgType, data); err != nil {
				return
			}
		}
	})

	srv := httptest.NewServer(handler)

	// Convert http://127.0.0.1:port to ws://127.0.0.1:port.
	wsURL := "ws" + srv.URL[len("http"):]
	return srv, wsURL
}

func TestDialWSAndEcho(t *testing.T) {
	srv, wsURL := wsEchoServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := DialWS(ctx, WSConfig{
		URL: wsURL,
		Log: newTestLogger(t),
	})
	if err != nil {
		t.Fatalf("DialWS failed: %v", err)
	}
	defer client.Close(websocket.StatusNormalClosure, "test complete")

	const payload = "hello over websocket"

	if err := client.Write(ctx, websocket.MessageText, []byte(payload)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	msgType, data, err := client.Read(ctx)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if msgType != websocket.MessageText {
		t.Fatalf("unexpected message type: %v", msgType)
	}
	if string(data) != payload {
		t.Fatalf("unexpected payload: got %q, want %q", string(data), payload)
	}
}

func TestProbeWS(t *testing.T) {
	srv, wsURL := wsEchoServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ProbeWS(ctx, wsURL); err != nil {
		t.Fatalf("ProbeWS failed: %v", err)
	}
}
