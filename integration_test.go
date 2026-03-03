package ydb

import (
	"bytes"
	"testing"
	"time"
)

func TestWsSingleClientUpdate(t *testing.T) {
	ts := newTestServer(t)
	roomname := "single-client"

	c := ts.dial(t, roomname)

	payload := []byte("hello-from-client")
	c.sendSyncUpdate(payload)

	// Wait for data to appear in store
	waitFor(t, 2*time.Second, func() bool {
		size, _ := ts.store.Size(YjsRoomName(roomname))
		return size > 0
	})

	data, _, err := ts.store.ReadFrom(YjsRoomName(roomname), 0)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected data in store")
	}

	// The stored data is framed: [outerLen][messageSync][messageYjsUpdate][innerLen][payload]
	// Unwrap the outer frame
	reader := bytes.NewReader(data)
	storedMsg, err := readPayload(reader)
	if err != nil {
		t.Fatalf("readPayload failed: %v", err)
	}

	// Parse the inner sync message
	syncType, innerPayload, err := parseSyncMessage(storedMsg)
	if err != nil {
		t.Fatalf("parseSyncMessage failed: %v", err)
	}
	if syncType != messageYjsUpdate {
		t.Fatalf("expected syncType %d, got %d", messageYjsUpdate, syncType)
	}
	if !bytes.Equal(innerPayload, payload) {
		t.Fatalf("payload mismatch: got %v, want %v", innerPayload, payload)
	}
}

func TestWsTwoClientSync(t *testing.T) {
	ts := newTestServer(t)
	roomname := "two-client"

	clientA := ts.dial(t, roomname)
	clientB := ts.dial(t, roomname)

	// Give subscriptions time to establish
	time.Sleep(100 * time.Millisecond)

	payload := []byte("update-from-A")
	clientA.sendSyncUpdate(payload)

	// Client B should receive the broadcast
	msg, ok := clientB.recv(2 * time.Second)
	if !ok {
		t.Fatalf("client B timed out waiting for message")
	}

	// The received message is the raw sync message: [messageSync][syncSubType][len][payload]
	syncType, innerPayload, err := parseSyncMessage(msg)
	if err != nil {
		t.Fatalf("parseSyncMessage failed: %v", err)
	}
	if syncType != messageYjsUpdate {
		t.Fatalf("expected syncType %d, got %d", messageYjsUpdate, syncType)
	}
	if !bytes.Equal(innerPayload, payload) {
		t.Fatalf("payload mismatch: got %v, want %v", innerPayload, payload)
	}
}

func TestWsLateJoinerCatchup(t *testing.T) {
	ts := newTestServer(t)
	roomname := "late-joiner"

	clientA := ts.dial(t, roomname)
	time.Sleep(50 * time.Millisecond)

	payload := []byte("data-before-B-joins")
	clientA.sendSyncUpdate(payload)

	// Wait for store persistence
	waitFor(t, 2*time.Second, func() bool {
		size, _ := ts.store.Size(YjsRoomName(roomname))
		return size > 0
	})

	// Now client B joins — should get catch-up
	clientB := ts.dial(t, roomname)
	msg, ok := clientB.recv(2 * time.Second)
	if !ok {
		t.Fatalf("client B timed out waiting for catch-up")
	}

	syncType, innerPayload, err := parseSyncMessage(msg)
	if err != nil {
		t.Fatalf("parseSyncMessage failed: %v", err)
	}
	if syncType != messageYjsUpdate {
		t.Fatalf("expected syncType %d, got %d", messageYjsUpdate, syncType)
	}
	if !bytes.Equal(innerPayload, payload) {
		t.Fatalf("catch-up payload mismatch: got %v, want %v", innerPayload, payload)
	}
}

func TestWsDisconnectCleanup(t *testing.T) {
	ts := newTestServer(t)
	roomname := "disconnect"

	c := ts.dial(t, roomname)
	time.Sleep(50 * time.Millisecond)

	// Count sessions before
	ts.ydb.sessionsMux.Lock()
	sessionsBefore := len(ts.ydb.sessions)
	ts.ydb.sessionsMux.Unlock()

	if sessionsBefore == 0 {
		t.Fatalf("expected at least 1 session")
	}

	c.close()

	// Wait for session cleanup
	waitFor(t, 2*time.Second, func() bool {
		ts.ydb.sessionsMux.Lock()
		n := len(ts.ydb.sessions)
		ts.ydb.sessionsMux.Unlock()
		return n < sessionsBefore
	})
}

func TestWsMultipleRoomsIsolation(t *testing.T) {
	ts := newTestServer(t)

	clientA := ts.dial(t, "roomA")
	clientB := ts.dial(t, "roomB")
	time.Sleep(100 * time.Millisecond)

	clientA.sendSyncUpdate([]byte("only-for-A"))

	// Client B should NOT receive the message
	_, ok := clientB.recv(200 * time.Millisecond)
	if ok {
		t.Fatalf("client B should not receive messages from room A")
	}

	// Verify data only in room A's store
	waitFor(t, 2*time.Second, func() bool {
		size, _ := ts.store.Size(YjsRoomName("roomA"))
		return size > 0
	})

	sizeB, _ := ts.store.Size(YjsRoomName("roomB"))
	if sizeB != 0 {
		t.Fatalf("room B store should be empty, got size %d", sizeB)
	}
}

func TestWsReconnectCatchup(t *testing.T) {
	ts := newTestServer(t)
	roomname := "reconnect"

	// First connection writes data
	c1 := ts.dial(t, roomname)
	time.Sleep(50 * time.Millisecond)
	c1.sendSyncUpdate([]byte("persistent-data"))

	waitFor(t, 2*time.Second, func() bool {
		size, _ := ts.store.Size(YjsRoomName(roomname))
		return size > 0
	})

	// Disconnect
	c1.close()
	time.Sleep(50 * time.Millisecond)

	// Reconnect — should catch up
	c2 := ts.dial(t, roomname)
	msg, ok := c2.recv(2 * time.Second)
	if !ok {
		t.Fatalf("reconnected client timed out waiting for catch-up")
	}

	_, innerPayload, err := parseSyncMessage(msg)
	if err != nil {
		t.Fatalf("parseSyncMessage failed: %v", err)
	}
	if !bytes.Equal(innerPayload, []byte("persistent-data")) {
		t.Fatalf("catch-up payload mismatch: got %v", innerPayload)
	}
}
