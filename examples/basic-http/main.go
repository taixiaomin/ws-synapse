package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"time"

	ws "github.com/taixiaomin/ws-synapse"
)

// MyHandler implements ws.EventHandler
type MyHandler struct {
	hub *ws.Hub
}

func (h *MyHandler) OnConnect(ctx context.Context, conn *ws.Conn) error {
	log.Printf("Client connected: %s (reconnect: %v)", conn.ID(), conn.IsReconnect())
	return conn.SendJSON(map[string]interface{}{
		"type":  "welcome",
		"token": conn.Token(),
	})
}

func (h *MyHandler) OnMessage(ctx context.Context, conn *ws.Conn, msg *ws.Message) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Raw, &payload); err != nil {
		return err
	}

	log.Printf("Received message from %s: %s", conn.ID(), msg.Type)

	switch msg.Type {
	case "subscribe":
		topic, ok := payload["topic"].(string)
		if !ok {
			return conn.SendJSON(map[string]string{"error": "missing topic"})
		}
		h.hub.Subscribe(ctx, conn.ID(), topic)
		return conn.SendJSON(map[string]string{"status": "subscribed", "topic": topic})

	case "ping":
		return conn.SendJSON(map[string]string{"type": "pong"})

	default:
		return nil
	}
}

func (h *MyHandler) OnDisconnect(ctx context.Context, conn *ws.Conn) {
	log.Printf("Client disconnected: %s", conn.ID())
}

func (h *MyHandler) OnError(ctx context.Context, conn *ws.Conn, err error) {
	log.Printf("Error from %s: %v", conn.ID(), err)
}

// OnSubscribe Optional: implement TopicEventHandler for topic lifecycle callbacks
func (h *MyHandler) OnSubscribe(ctx context.Context, connID, topic string) {
	log.Printf("Client %s subscribed to %s", connID, topic)
}

func (h *MyHandler) OnUnsubscribe(ctx context.Context, connID, topic string) {
	log.Printf("Client %s unsubscribed from %s", connID, topic)
}

func main() {
	// Create event handler
	handler := &MyHandler{}

	// Create server with options
	server := ws.NewServer(handler,
		ws.WithTokenProvider(ws.NewMemoryTokenProvider()),
		ws.WithPingInterval(30*time.Second),
		ws.WithMaxConnections(10000),
		ws.WithRateLimit(100, 20),
		ws.WithInsecureSkipVerify(true), // dev only!
		ws.WithLogger(ws.NewSlogAdapter(slog.Default())),
	)

	// Inject Hub reference so handler can use it
	handler.hub = server.Hub()

	// Setup HTTP routes
	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		connID := r.URL.Query().Get("id")
		server.Upgrade(w, r, connID)
	})

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"connections": server.Hub().ConnCount(),
		})
	})

	// Start server
	log.Println("Starting WebSocket server on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
