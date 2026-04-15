package core

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

// NewServer creates a Server with the given EventHandler and options.
func NewServer(handler EventHandler, opts ...ServerOption) *Server {
	o := defaultServerOptions()
	for _, fn := range opts {
		fn(&o)
	}
	o.validate()

	m := metricsOrNop(o.metrics)

	// Build Hub options from Server options.
	hubOpts := []HubOption{
		WithHubLogger(o.logger),
		WithHubMaxConns(o.maxConnections),
		WithHubMetrics(m),
		WithHubDrainTimeout(o.drainTimeout),
	}
	if o.pendingStore != nil {
		hubOpts = append(hubOpts, WithHubPendingStore(o.pendingStore))
	}
	if o.tokenProvider != nil {
		hubOpts = append(hubOpts, WithHubTokenProvider(o.tokenProvider))
	}
	// If handler also implements TopicEventHandler, wire it up.
	if th, ok := handler.(TopicEventHandler); ok {
		hubOpts = append(hubOpts, WithHubTopicHandler(th))
	}

	if o.clusterRelay != nil {
		hubOpts = append(hubOpts, WithHubClusterRelay(o.clusterRelay))
	}

	hub := NewHub(hubOpts...)

	// Start the cluster relay after Hub creation so relay can call back into Hub.
	if o.clusterRelay != nil {
		if err := o.clusterRelay.Start(hub); err != nil {
			o.logger.Error("cluster relay start failed", "error", err)
		}
	}

	// Resolve message parser.
	parser := o.messageParser
	if parser == nil {
		parser = jsonTypeParser{}
	}

	return &Server{
		hub:     hub,
		handler: handler,
		opts:    o,
		metrics: m,
		parser:  parser,
	}
}

// Hub returns the underlying Hub for direct access (Send, Broadcast, Subscribe, etc.).
func (s *Server) Hub() *Hub { return s.hub }

// Shutdown gracefully closes all connections and waits for read/write pumps to
// finish draining. The context controls the maximum time to wait.
// An optional finalMessage is broadcast to all connections before closing.
func (s *Server) Shutdown(ctx context.Context, finalMessage ...[]byte) {
	// Snapshot all conns BEFORE Hub.Shutdown clears the map,
	// so the timeout branch can force-close them if needed.
	var allConns []*Conn
	s.hub.conns.Range(func(_, value interface{}) bool {
		allConns = append(allConns, value.(*Conn))
		return true
	})

	s.hub.Shutdown(ctx, finalMessage...)

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// Timeout: force-close all connections to unblock stuck readPump goroutines.
		for _, c := range allConns {
			c.CloseNow()
		}
	}
}

// HandleHTTP is a convenience net/http handler that extracts the connection ID
// using the configured ConnIDExtractor (or OnUpgrade hook) and upgrades to WebSocket.
func (s *Server) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	// If OnUpgrade is configured, it replaces authenticator + connIDExtractor.
	if s.opts.onUpgrade != nil {
		info, err := s.opts.onUpgrade(r)
		if err != nil {
			jsonError(w, err.Error(), http.StatusUnauthorized)
			return
		}
		if info == nil || info.ConnID == "" {
			jsonError(w, "connection id is required", http.StatusBadRequest)
			return
		}
		s.upgrade(w, r, info.ConnID, nil, info.Metadata)
		return
	}

	if s.opts.connIDExtractor == nil {
		jsonError(w, "connIDExtractor not configured; use Upgrade() or set WithConnIDExtractor", http.StatusInternalServerError)
		return
	}

	connID, err := s.opts.connIDExtractor(r)
	if err != nil || connID == "" {
		jsonError(w, "connection id is required", http.StatusBadRequest)
		return
	}

	s.upgrade(w, r, connID, nil, nil)
}

// Upgrade handles the WebSocket upgrade from any HTTP handler.
// connID must be pre-extracted by the caller. This method is framework-agnostic.
func (s *Server) Upgrade(w http.ResponseWriter, r *http.Request, connID string, opts ...UpgradeOption) {
	if connID == "" {
		jsonError(w, "connection id is required", http.StatusBadRequest)
		return
	}

	var uo *upgradeOpts
	if len(opts) > 0 {
		uo = &upgradeOpts{}
		for _, fn := range opts {
			fn(uo)
		}
	}

	s.upgrade(w, r, connID, uo, nil)
}

// UpgradeWithMeta handles the WebSocket upgrade with additional metadata to inject.
// connID must be pre-extracted by the caller.
func (s *Server) UpgradeWithMeta(w http.ResponseWriter, r *http.Request, connID string, metadata map[string]interface{}, opts ...UpgradeOption) {
	if connID == "" {
		jsonError(w, "connection id is required", http.StatusBadRequest)
		return
	}

	var uo *upgradeOpts
	if len(opts) > 0 {
		uo = &upgradeOpts{}
		for _, fn := range opts {
			fn(uo)
		}
	}

	s.upgrade(w, r, connID, uo, metadata)
}

// upgrade is the shared implementation.
func (s *Server) upgrade(w http.ResponseWriter, r *http.Request, connID string, uo *upgradeOpts, metadata map[string]interface{}) {
	// ── 1. Pre-check connection limit
	if s.opts.maxConnections > 0 &&
		s.hub.ConnCount() >= int(s.opts.maxConnections) &&
		s.hub.GetConn(connID) == nil {
		jsonError(w, "max connections reached", http.StatusServiceUnavailable)
		return
	}

	// ── 2. Authentication
	if s.opts.authenticator != nil {
		if err := s.opts.authenticator(r); err != nil {
			jsonError(w, err.Error(), http.StatusUnauthorized)
			return
		}
	}

	// ── 3. WebSocket upgrade
	acceptOpts := &websocket.AcceptOptions{
		InsecureSkipVerify: s.opts.insecureSkipVerify,
	}
	if len(s.opts.allowedOrigins) > 0 {
		acceptOpts.OriginPatterns = s.opts.allowedOrigins
	}
	wsConn, err := websocket.Accept(w, r, acceptOpts)
	if err != nil {
		return
	}
	wsConn.SetReadLimit(s.opts.maxMessageSize)

	// ── 4. Token handling
	// Use the HTTP request context only for short-lived pre-upgrade operations
	// (token validation, generation). The pump goroutines get an independent
	// context so they are not cancelled when the HTTP handler returns.
	httpCtx := r.Context()

	// Resolve token: explicit UpgradeOption > query string fallback.
	tok := ""
	if uo != nil && uo.token != "" {
		tok = uo.token
	} else {
		tok = r.URL.Query().Get(s.opts.tokenParam)
	}

	isReconnect := false
	if s.opts.tokenProvider != nil {
		if tok != "" {
			valid, vErr := s.opts.tokenProvider.Validate(httpCtx, connID, tok)
			if vErr != nil || !valid {
				_ = wsConn.Close(websocket.StatusPolicyViolation, "invalid reconnect token")
				return
			}
			isReconnect = true
			// Explicitly revoke old token before generating a new one, so custom
			// TokenProvider implementations that don't overwrite on Generate are safe.
			_ = s.opts.tokenProvider.Revoke(httpCtx, connID)
			tok, err = s.opts.tokenProvider.Generate(httpCtx, connID)
			if err != nil {
				_ = wsConn.Close(websocket.StatusInternalError, "token rotation failed")
				return
			}
		} else {
			tok, err = s.opts.tokenProvider.Generate(httpCtx, connID)
			if err != nil {
				_ = wsConn.Close(websocket.StatusInternalError, "token generation failed")
				return
			}
		}
	}

	// ── 5. Create connection
	var limiter *rate.Limiter
	if s.opts.rateLimitPerSec > 0 {
		limiter = rate.NewLimiter(rate.Limit(s.opts.rateLimitPerSec), s.opts.rateLimitBurst)
	}
	conn := NewConn(connID, tok, wsConn, s.opts.sendChannelSize, limiter)
	conn.isReconnect = isReconnect

	// Snapshot HTTP request info so OnConnect can access query params, headers, etc.
	conn.info = &ConnInfo{
		RemoteAddr: r.RemoteAddr,
		Header:     r.Header.Clone(),
		Query:      r.URL.Query(),
	}

	// Inject metadata from OnUpgrade or UpgradeWithMeta.
	if metadata != nil {
		for k, v := range metadata {
			conn.Set(k, v)
		}
	}

	// ── 6. Inject Hub reference BEFORE OnConnect so conn.Hub() is usable.
	conn.hub = s.hub

	// ── 7. Fire OnConnect (conn.Hub() is now available)
	if err := s.onConnect(httpCtx, conn); err != nil {
		conn.hub = nil
		// Revoke the token we just generated so it cannot be reused.
		if s.opts.tokenProvider != nil && tok != "" {
			_ = s.opts.tokenProvider.Revoke(httpCtx, connID)
		}
		_ = wsConn.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}

	// ── 8. Register in Hub (replaces old conn, flushes pending)
	if err := s.hub.register(httpCtx, conn); err != nil {
		conn.hub = nil
		if s.opts.tokenProvider != nil && tok != "" {
			_ = s.opts.tokenProvider.Revoke(httpCtx, connID)
		}
		_ = wsConn.Close(websocket.StatusTryAgainLater, err.Error())
		return
	}

	if isReconnect {
		s.metrics.IncReconnects()
	}

	// ── 9. Start pumps with an independent context (not tied to HTTP request).
	// This prevents framework-specific request context cancellation from killing
	// long-lived WebSocket connections. The context is cancelled when readPump exits.
	connCtx, connCancel := context.WithCancel(context.Background())
	s.wg.Add(2)
	go s.writePump(connCtx, conn)
	s.readPump(connCtx, connCancel, conn) // blocks
}

// readPump reads messages from the WebSocket and dispatches to EventHandler.
// connCancel is called when readPump exits to signal writePump to stop.
func (s *Server) readPump(ctx context.Context, connCancel context.CancelFunc, c *Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.opts.logger.Error("panic in readPump", "connID", c.ID(), "panic", r)
			s.metrics.IncErrors()
		}

		removed := s.hub.unregister(c)
		c.Finish()
		c.CloseNow()

		// Use a fresh context for disconnect cleanup because the original HTTP
		// request context may already be cancelled at this point.
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), s.opts.drainTimeout)
		s.onDisconnect(disconnectCtx, c)
		disconnectCancel()

		if removed && s.opts.tokenProvider != nil {
			_ = s.opts.tokenProvider.Revoke(context.Background(), c.ID())
		}

		// Cancel the connection context to signal writePump to stop,
		// then mark this goroutine as done.
		connCancel()
		s.wg.Done()
	}()

	for {
		_, data, err := c.ws.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure &&
				websocket.CloseStatus(err) != websocket.StatusGoingAway {
				s.onError(ctx, c, err)
				s.metrics.IncErrors()
			}
			return
		}

		s.metrics.IncMessagesIn()

		if !c.AllowMessage() {
			s.onError(ctx, c, ErrRateLimited)
			continue
		}

		// Parse message type using the configured parser.
		msg := &Message{Raw: data}
		msg.Type = s.parser.ParseType(data)

		if err := s.onMessage(ctx, c, msg); err != nil {
			s.onError(ctx, c, err)
			s.metrics.IncErrors()
		}
	}
}

// writePump drains sendCh and handles heartbeat pings.
func (s *Server) writePump(ctx context.Context, c *Conn) {
	ticker := time.NewTicker(s.opts.pingInterval)
	defer func() {
		if r := recover(); r != nil {
			s.opts.logger.Error("panic in writePump", "connID", c.ID(), "panic", r)
			s.metrics.IncErrors()
		}
		ticker.Stop()
		s.drainToPending(c)

		// Force-close the underlying WebSocket so that readPump's blocking
		// c.ws.Read() returns immediately with an error. Without this,
		// a dead connection (detected by ping failure or write error) would
		// linger in the Hub until TCP keepalive times out (often 2+ hours),
		// preventing unregister, onDisconnect, and token revocation.
		c.CloseNow()

		s.wg.Done()
	}()

	for {
		select {
		case data := <-c.sendCh:
			if err := c.WriteEnvelope(ctx, data, s.opts.writeTimeout); err != nil {
				return
			}
			s.metrics.IncMessagesOut()
			// Batch flush: write any messages already queued in the buffer.
			for n := len(c.sendCh); n > 0; n-- {
				if err := c.WriteEnvelope(ctx, <-c.sendCh, s.opts.writeTimeout); err != nil {
					return
				}
				s.metrics.IncMessagesOut()
			}

			// After draining sendCh, check for overflow messages in PendingStore.
			if c.HasOverflow() {
				s.drainOverflow(ctx, c)
			}

		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, s.opts.writeTimeout)
			err := c.ws.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}

		case <-c.Done():
			return

		case <-ctx.Done():
			return
		}
	}
}

// drainToPending saves remaining messages in sendCh to the pending store.
// Signals c.MarkDrained() when done so Hub.register() can synchronize.
func (s *Server) drainToPending(c *Conn) {
	defer c.MarkDrained()

	if s.hub.pendingStore == nil {
		return
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), s.opts.drainTimeout)
	defer cancel()
	for {
		select {
		case env := <-c.sendCh:
			pm := PendingMessage{Data: env.Data, MsgType: envelopeToMsgType(env)}
			if err := s.hub.pendingStore.PushEnvelope(drainCtx, c.ID(), pm); err != nil {
				s.hub.logger.Error("drainToPending: push failed", "connID", c.ID(), "error", err)
				return
			}
		case <-drainCtx.Done():
			return
		default:
			return
		}
	}
}

// drainOverflow pulls overflow messages from PendingStore and writes them to the WebSocket.
// Called by writePump after sendCh is drained when the overflow flag is set.
func (s *Server) drainOverflow(ctx context.Context, c *Conn) {
	if s.hub.pendingStore == nil {
		c.clearOverflow()
		return
	}
	msgs, err := s.hub.pendingStore.PopAll(ctx, c.ID())
	if err != nil {
		s.hub.logger.Warn("drainOverflow: PopAll failed", "connID", c.ID(), "error", err)
		return // Don't clear overflow — retry on next sendCh drain.
	}
	if len(msgs) == 0 {
		c.clearOverflow()
		return
	}
	for i, m := range msgs {
		msgType := websocket.MessageText
		if m.MsgType == MsgTypeBinary {
			msgType = websocket.MessageBinary
		}
		env := MessageEnvelope{Data: m.Data, MsgType: msgType}
		if err := c.WriteEnvelope(ctx, env, s.opts.writeTimeout); err != nil {
			// Write failed — re-push remaining messages back to PendingStore.
			for _, remaining := range msgs[i:] {
				if pushErr := s.hub.pendingStore.PushEnvelope(ctx, c.ID(), remaining); pushErr != nil {
					s.hub.logger.Error("drainOverflow: re-push failed", "connID", c.ID(), "error", pushErr)
					return
				}
			}
			return
		}
		s.metrics.IncMessagesOut()
	}
	c.clearOverflow()
}

// envelopeToMsgType converts a websocket.MessageType to the PendingMessage MsgType constant.
func envelopeToMsgType(env MessageEnvelope) int {
	if env.MsgType == websocket.MessageBinary {
		return MsgTypeBinary
	}
	return MsgTypeText
}

// ── Panic-safe event handler wrappers ──────────────────────────────────────

func (s *Server) onConnect(ctx context.Context, c *Conn) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("OnConnect panic: %v", r)
			s.opts.logger.Error("panic in OnConnect", "connID", c.ID(), "panic", r)
			s.metrics.IncErrors()
		}
	}()
	return s.handler.OnConnect(ctx, c)
}

func (s *Server) onMessage(ctx context.Context, c *Conn, msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("OnMessage panic: %v", r)
			s.opts.logger.Error("panic in OnMessage", "connID", c.ID(), "panic", r)
			s.metrics.IncErrors()
		}
	}()
	return s.handler.OnMessage(ctx, c, msg)
}

func (s *Server) onDisconnect(ctx context.Context, c *Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.opts.logger.Error("panic in OnDisconnect", "connID", c.ID(), "panic", r)
			s.metrics.IncErrors()
		}
	}()
	s.handler.OnDisconnect(ctx, c)
}

func (s *Server) onError(ctx context.Context, c *Conn, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.opts.logger.Error("panic in OnError", "connID", c.ID(), "panic", r)
		}
	}()
	s.handler.OnError(ctx, c, err)
}
