package ydb

import (
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetOrCreateRoomConcurrent(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	var wg sync.WaitGroup
	p := 100
	var numOfTests uint64 = 10000
	wg.Add(p)
	for i := range p {
		go func(seed int) {
			defer wg.Done()
			src := rand.NewSource(int64(seed))
			r := rand.New(src)
			for j := uint64(0); j < numOfTests; j++ {
				roomname := YjsRoomName(strconv.FormatUint(r.Uint64()%numOfTests, 10))
				ydbInstance.getOrCreateRoom(roomname)
			}
		}(i)
	}
	wg.Wait()
}

func TestGetOrCreateRoomIdempotent(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	name := YjsRoomName("myroom")
	r1 := ydbInstance.getOrCreateRoom(name)
	r2 := ydbInstance.getOrCreateRoom(name)
	if r1 != r2 {
		t.Fatalf("expected same room pointer, got different")
	}
}

func TestSessionCreateRemove(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	s := ydbInstance.createSession("testroom")
	if s == nil {
		t.Fatalf("expected session, got nil")
	}

	got := ydbInstance.getSession(s.sessionid)
	if got != s {
		t.Fatalf("expected to find session by id")
	}

	ydbInstance.removeSession(s.sessionid)
	got = ydbInstance.getSession(s.sessionid)
	if got != nil {
		t.Fatalf("expected nil after remove, got session")
	}
}

func TestRoomReaperCleansIdle(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := Config{
		SendBufferSize:   256,
		MaxMessageSize:   10 * 1024 * 1024,
		MaxRoomSize:      50 * 1024 * 1024,
		BroadcastBuffer:  64,
		RoomIdleTimeout:  50 * time.Millisecond,
		RoomReapInterval: 25 * time.Millisecond,
	}
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	name := YjsRoomName("ephemeral")
	ydbInstance.getOrCreateRoom(name)

	// Verify room exists
	ydbInstance.roomsMux.RLock()
	_, exists := ydbInstance.rooms[name]
	ydbInstance.roomsMux.RUnlock()
	if !exists {
		t.Fatalf("room should exist after creation")
	}

	// Wait for reaper to clean it up (idle timeout + reap interval + buffer)
	waitFor(t, 500*time.Millisecond, func() bool {
		ydbInstance.roomsMux.RLock()
		_, exists := ydbInstance.rooms[name]
		ydbInstance.roomsMux.RUnlock()
		return !exists
	})
}

func TestRoomReaperKeepsActive(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := Config{
		SendBufferSize:   256,
		MaxMessageSize:   10 * 1024 * 1024,
		MaxRoomSize:      50 * 1024 * 1024,
		BroadcastBuffer:  64,
		RoomIdleTimeout:  50 * time.Millisecond,
		RoomReapInterval: 25 * time.Millisecond,
	}
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	name := YjsRoomName("active-room")
	r := ydbInstance.getOrCreateRoom(name)
	atomic.AddInt32(&r.subCount, 1) // simulate active subscriber

	// Wait longer than idle timeout
	time.Sleep(150 * time.Millisecond)

	ydbInstance.roomsMux.RLock()
	_, exists := ydbInstance.rooms[name]
	ydbInstance.roomsMux.RUnlock()
	if !exists {
		t.Fatalf("room with active subscriber should not be reaped")
	}

	atomic.AddInt32(&r.subCount, -1)
}

func TestUpdateRoomPersistsToStore(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	roomname := YjsRoomName("persist-room")
	s := ydbInstance.createSession(string(roomname))

	// Build a sync update message: [messageSync][messageYjsUpdate][len][payload]
	payload := []byte("test-update-data")
	msgBuf := &bytes.Buffer{}
	writeUvarint(msgBuf, messageSync)
	writeUvarint(msgBuf, messageYjsUpdate)
	writePayload(msgBuf, payload)

	ydbInstance.updateRoom(roomname, s, msgBuf.Bytes())

	// Verify data is in store (framed with writePayload)
	data, offset, err := store.ReadFrom(roomname, 0)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if offset == 0 || len(data) == 0 {
		t.Fatalf("expected data in store, got offset=%d len=%d", offset, len(data))
	}

	// The stored data is writePayload(msgBuf.Bytes()), i.e., [len][msg]
	// Read it back using readPayload
	reader := bytes.NewReader(data)
	storedMsg, err := readPayload(reader)
	if err != nil {
		t.Fatalf("readPayload failed: %v", err)
	}
	if !bytes.Equal(storedMsg, msgBuf.Bytes()) {
		t.Fatalf("stored message doesn't match: got %v, want %v", storedMsg, msgBuf.Bytes())
	}
}

func TestUpdateRoomRejectsOversizedRoom(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	cfg.MaxRoomSize = 100
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	roomname := YjsRoomName("size-limited-room")
	s := ydbInstance.createSession(string(roomname))

	// Build a payload that will exceed MaxRoomSize
	bigPayload := make([]byte, 80)
	for i := range bigPayload {
		bigPayload[i] = 'A'
	}
	msgBuf := &bytes.Buffer{}
	writeUvarint(msgBuf, messageSync)
	writeUvarint(msgBuf, messageYjsUpdate)
	writePayload(msgBuf, bigPayload)

	// First write succeeds
	ydbInstance.updateRoom(roomname, s, msgBuf.Bytes())
	size, _ := store.Size(roomname)
	if size == 0 {
		t.Fatalf("first write should succeed")
	}

	// Second write should be rejected (would exceed MaxRoomSize)
	sizeBefore, _ := store.Size(roomname)
	ydbInstance.updateRoom(roomname, s, msgBuf.Bytes())
	sizeAfter, _ := store.Size(roomname)
	if sizeAfter != sizeBefore {
		t.Fatalf("second write should be rejected: size went from %d to %d (max %d)", sizeBefore, sizeAfter, cfg.MaxRoomSize)
	}
}

func TestReadUpdateMessageRejectsOversized(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	cfg.MaxMessageSize = 50
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	s := ydbInstance.createSession("testroom")

	// Build a message with payload larger than MaxMessageSize
	bigPayload := make([]byte, 100)
	msgBuf := &bytes.Buffer{}
	writeUvarint(msgBuf, messageSync)
	writeUvarint(msgBuf, messageYjsUpdate)
	writePayload(msgBuf, bigPayload)

	err := ydbInstance.readMessage(msgBuf, s)
	if err == nil {
		t.Fatalf("expected error for oversized message")
	}

	// Store should be empty — nothing persisted
	size, _ := store.Size(YjsRoomName("testroom"))
	if size != 0 {
		t.Fatalf("oversized message should not be persisted, store size=%d", size)
	}
}

func TestSubscribeRoomCatchup(t *testing.T) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	roomname := YjsRoomName("catchup-room")

	// Write data to store first (simulating a previous update)
	payload := []byte("pre-existing-data")
	msgBuf := &bytes.Buffer{}
	writeUvarint(msgBuf, messageSync)
	writeUvarint(msgBuf, messageYjsUpdate)
	writePayload(msgBuf, payload)

	s1 := ydbInstance.createSession(string(roomname))
	ydbInstance.updateRoom(roomname, s1, msgBuf.Bytes())

	// Create a new session and subscribe — should receive catch-up
	s2 := ydbInstance.createSession(string(roomname))
	mc := &mockConn{}
	s2.setConn(mc)

	ydbInstance.subscribeRoom(s2, 0)

	// Wait for catch-up to be delivered
	waitFor(t, time.Second, func() bool {
		msgs := mc.getMessages()
		return len(msgs) > 0
	})

	msgs := mc.getMessages()
	if len(msgs) == 0 {
		t.Fatalf("expected catch-up messages, got none")
	}

	// The catch-up message should be the stored sync message
	if !bytes.Equal(msgs[0], msgBuf.Bytes()) {
		t.Fatalf("catch-up message mismatch: got %v, want %v", msgs[0], msgBuf.Bytes())
	}
}
