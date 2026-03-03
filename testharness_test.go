package ydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// --- MemoryStore ---

type MemoryStore struct {
	mu          sync.RWMutex
	data        map[YjsRoomName][]byte
	maxRoomSize uint32
}

func newMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[YjsRoomName][]byte)}
}

func (ms *MemoryStore) Append(room YjsRoomName, data []byte) (uint32, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.maxRoomSize > 0 {
		currentSize := uint32(len(ms.data[room]))
		if currentSize+uint32(len(data)) > ms.maxRoomSize {
			return currentSize, fmt.Errorf("room %s exceeds max size %d", room, ms.maxRoomSize)
		}
	}

	ms.data[room] = append(ms.data[room], data...)
	return uint32(len(ms.data[room])), nil
}

func (ms *MemoryStore) ReadFrom(room YjsRoomName, offset uint32) ([]byte, uint32, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	d := ms.data[room]
	if d == nil {
		return nil, 0, nil
	}
	size := uint32(len(d))
	if offset >= size {
		return nil, size, nil
	}
	result := make([]byte, size-offset)
	copy(result, d[offset:])
	return result, size, nil
}

func (ms *MemoryStore) Size(room YjsRoomName) (uint32, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return uint32(len(ms.data[room])), nil
}

func (ms *MemoryStore) SetInitialContent(room YjsRoomName, data []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	d := make([]byte, len(data))
	copy(d, data)
	ms.data[room] = d
	return nil
}

// --- mockConn ---

type mockConn struct {
	mu       sync.Mutex
	messages [][]byte
}

func (mc *mockConn) WriteMessage(m []byte, pm *websocket.PreparedMessage) {
	mc.mu.Lock()
	msg := make([]byte, len(m))
	copy(msg, m)
	mc.messages = append(mc.messages, msg)
	mc.mu.Unlock()
}

func (mc *mockConn) getMessages() [][]byte {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	msgs := make([][]byte, len(mc.messages))
	copy(msgs, mc.messages)
	return msgs
}

// --- testServer ---

type testServer struct {
	ydb        *Ydb
	httpServer *httptest.Server
	wsURL      string
	store      Store
	broadcaster Broadcaster
}

func newTestServer(t *testing.T) *testServer {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := Config{
		SendBufferSize:   256,
		MaxMessageSize:   10 * 1024 * 1024,
		MaxRoomSize:      50 * 1024 * 1024,
		BroadcastBuffer:  64,
		RoomIdleTimeout:  200 * time.Millisecond,
		RoomReapInterval: 100 * time.Millisecond,
	}
	return newTestServerWithComponents(t, store, broadcaster, cfg)
}

func newDiskTestServer(t *testing.T) *testServer {
	dir := t.TempDir()
	store := NewDiskStore(dir, WithMaxRoomSize(50*1024*1024))
	broadcaster := NewLocalBroadcaster(64)
	cfg := Config{
		SendBufferSize:   256,
		MaxMessageSize:   10 * 1024 * 1024,
		MaxRoomSize:      50 * 1024 * 1024,
		BroadcastBuffer:  64,
		RoomIdleTimeout:  200 * time.Millisecond,
		RoomReapInterval: 100 * time.Millisecond,
	}
	return newTestServerWithComponents(t, store, broadcaster, cfg)
}

func newTestServerWithConfig(t *testing.T, cfg Config) *testServer {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(cfg.BroadcastBuffer)
	return newTestServerWithComponents(t, store, broadcaster, cfg)
}

func newTestServerWithComponents(t *testing.T, store Store, broadcaster Broadcaster, cfg Config) *testServer {
	ydbInstance := InitYdb(store, broadcaster, cfg)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/", YdbWsConnectionHandler(ydbInstance))

	server := httptest.NewServer(mux)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ts := &testServer{
		ydb:         ydbInstance,
		httpServer:  server,
		wsURL:       wsURL,
		store:       store,
		broadcaster: broadcaster,
	}

	t.Cleanup(func() {
		ydbInstance.Close()
		server.Close()
	})

	return ts
}

// --- testWsClient ---

type testWsClient struct {
	t        *testing.T
	conn     *websocket.Conn
	received chan []byte
	done     chan struct{}
}

func (ts *testServer) dial(t *testing.T, roomname string) *testWsClient {
	url := ts.wsURL + "/ws/" + roomname
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("failed to dial %s: %v", url, err)
	}

	c := &testWsClient{
		t:        t,
		conn:     conn,
		received: make(chan []byte, 1000),
		done:     make(chan struct{}),
	}

	go func() {
		defer close(c.done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			c.received <- msg
		}
	}()

	t.Cleanup(func() {
		c.close()
	})

	return c
}

func (c *testWsClient) sendSyncUpdate(payload []byte) {
	msg := makeYjsSyncUpdate(payload)
	err := c.conn.WriteMessage(websocket.BinaryMessage, msg)
	if err != nil {
		c.t.Fatalf("sendSyncUpdate failed: %v", err)
	}
}

func (c *testWsClient) sendSyncStep1(stateVector []byte) {
	msg := makeYjsSyncStep1(stateVector)
	err := c.conn.WriteMessage(websocket.BinaryMessage, msg)
	if err != nil {
		c.t.Fatalf("sendSyncStep1 failed: %v", err)
	}
}

func (c *testWsClient) recv(timeout time.Duration) ([]byte, bool) {
	select {
	case msg := <-c.received:
		return msg, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (c *testWsClient) recvAll(timeout time.Duration) [][]byte {
	var msgs [][]byte
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case msg := <-c.received:
			msgs = append(msgs, msg)
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(timeout)
		case <-timer.C:
			return msgs
		}
	}
}

func (c *testWsClient) close() {
	c.conn.Close()
	<-c.done
}

// --- Message helpers ---

func makeYjsSyncUpdate(payload []byte) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageSync)
	writeUvarint(buf, messageYjsUpdate)
	writePayload(buf, payload)
	return buf.Bytes()
}

func makeYjsSyncStep1(stateVector []byte) []byte {
	buf := &bytes.Buffer{}
	writeUvarint(buf, messageSync)
	writeUvarint(buf, messageYjsSyncStep1)
	writePayload(buf, stateVector)
	return buf.Bytes()
}

func parseSyncMessage(msg []byte) (syncType uint64, payload []byte, err error) {
	buf := bytes.NewBuffer(msg)
	msgType, err := binary.ReadUvarint(buf)
	if err != nil {
		return 0, nil, err
	}
	if msgType != messageSync {
		return 0, nil, fmt.Errorf("not a sync message: type=%d", msgType)
	}
	syncType, err = binary.ReadUvarint(buf)
	if err != nil {
		return 0, nil, err
	}
	payload, err = readPayload(buf)
	return syncType, payload, err
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitFor timed out after %v", timeout)
}
