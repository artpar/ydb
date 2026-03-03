package ydb

import (
	"bytes"
	"testing"
	"time"
)

func TestClientSendAndReceive(t *testing.T) {
	ts := newTestServer(t)
	roomname := "client-test"

	// Use testWsClient to verify the full client->server->client flow
	clientA := ts.dial(t, roomname)
	clientB := ts.dial(t, roomname)
	time.Sleep(100 * time.Millisecond)

	payload := []byte("client-update")
	clientA.sendSyncUpdate(payload)

	msg, ok := clientB.recv(2 * time.Second)
	if !ok {
		t.Fatalf("client B timed out waiting for update")
	}

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

func TestClientMultipleUpdates(t *testing.T) {
	ts := newTestServer(t)
	roomname := "multi-update"

	clientA := ts.dial(t, roomname)
	clientB := ts.dial(t, roomname)
	time.Sleep(100 * time.Millisecond)

	payloads := [][]byte{
		[]byte("update-1"),
		[]byte("update-2"),
		[]byte("update-3"),
	}

	for _, p := range payloads {
		clientA.sendSyncUpdate(p)
	}

	received := clientB.recvAll(time.Second)
	if len(received) < len(payloads) {
		t.Fatalf("expected at least %d messages, got %d", len(payloads), len(received))
	}

	for i, msg := range received[:len(payloads)] {
		_, innerPayload, err := parseSyncMessage(msg)
		if err != nil {
			t.Fatalf("message %d: parseSyncMessage failed: %v", i, err)
		}
		if !bytes.Equal(innerPayload, payloads[i]) {
			t.Fatalf("message %d: payload mismatch: got %v, want %v", i, innerPayload, payloads[i])
		}
	}
}

func TestClientOldApiProtocol(t *testing.T) {
	// Verify the old client.go API sends correct protocol
	ts := newTestServer(t)
	roomname := "old-api"

	// Use testWsClient as receiver
	receiver := ts.dial(t, roomname)
	time.Sleep(100 * time.Millisecond)

	// Use old client API to send
	c := newClient()
	err := c.Connect(ts.wsURL + "/ws/" + roomname)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	c.currentRoom = YjsRoomName(roomname)

	c.UpdateRoom(YjsRoomName(roomname), []byte("old-api-data"))

	msg, ok := receiver.recv(2 * time.Second)
	if !ok {
		t.Fatalf("receiver timed out")
	}

	syncType, payload, err := parseSyncMessage(msg)
	if err != nil {
		t.Fatalf("parseSyncMessage failed: %v", err)
	}
	if syncType != messageYjsUpdate {
		t.Fatalf("expected syncType %d, got %d", messageYjsUpdate, syncType)
	}
	if !bytes.Equal(payload, []byte("old-api-data")) {
		t.Fatalf("payload mismatch: got %v", payload)
	}

	c.Disconnect()
}
