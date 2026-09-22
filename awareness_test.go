package ydb

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func sendAwareness(t *testing.T, client *testWsClient, states []awarenessState) {
	t.Helper()
	message, err := createAwarenessMessage(states)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
		t.Fatal(err)
	}
}

func parseAwareness(t *testing.T, message []byte) []awarenessState {
	t.Helper()
	reader := bytes.NewReader(message)
	messageType, err := binary.ReadUvarint(reader)
	if err != nil || messageType != messageAwareness {
		t.Fatalf("invalid awareness message: type=%d err=%v", messageType, err)
	}
	payload, err := readPayload(reader)
	if err != nil {
		t.Fatal(err)
	}
	states, err := decodeAwarenessPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return states
}

func TestAwarenessBroadcastsAllStatesWithoutPersistence(t *testing.T) {
	ts := newTestServer(t)
	clientA := ts.dial(t, "awareness")
	clientB := ts.dial(t, "awareness")
	time.Sleep(50 * time.Millisecond)

	want := []awarenessState{
		{clientID: 11, clock: 2, state: `{"user":{"name":"a"}}`},
		{clientID: 12, clock: 4, state: `{"cursor":{"index":7}}`},
	}
	sendAwareness(t, clientA, want)
	gotMessage, ok := clientB.recv(2 * time.Second)
	if !ok {
		t.Fatal("peer did not receive awareness")
	}
	got := parseAwareness(t, gotMessage)
	if len(got) != len(want) {
		t.Fatalf("received %d states, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("state %d = %#v, want %#v", i, got[i], want[i])
		}
	}
	if size, _ := ts.store.Size("awareness"); size != 0 {
		t.Fatalf("awareness was persisted: %d bytes", size)
	}
}

func TestAwarenessQueryAndDisconnectRemoval(t *testing.T) {
	ts := newTestServer(t)
	clientA := ts.dial(t, "presence")
	sendAwareness(t, clientA, []awarenessState{{clientID: 21, clock: 5, state: `{"user":{"name":"a"}}`}})

	clientB := ts.dial(t, "presence")
	query, ok := clientA.recv(2 * time.Second)
	if !ok {
		t.Fatal("existing peer did not receive awareness query")
	}
	messageType, err := binary.ReadUvarint(bytes.NewReader(query))
	if err != nil || messageType != messageQueryAwareness {
		t.Fatalf("message type = %d, err=%v", messageType, err)
	}

	sendAwareness(t, clientA, []awarenessState{{clientID: 21, clock: 6, state: `{"user":{"name":"a2"}}`}})
	if _, ok := clientB.recv(2 * time.Second); !ok {
		t.Fatal("peer did not receive awareness update")
	}
	clientA.close()

	removalMessage, ok := clientB.recv(2 * time.Second)
	if !ok {
		t.Fatal("peer did not receive disconnect removal")
	}
	removals := parseAwareness(t, removalMessage)
	if len(removals) != 1 || removals[0].clientID != 21 || removals[0].clock != 7 || removals[0].state != "null" {
		t.Fatalf("unexpected removals: %#v", removals)
	}
}

func TestSessionOnlyRemovesItsOwnAwarenessState(t *testing.T) {
	session := newSession(1, "presence")
	session.trackAwareness([]awarenessState{{clientID: 21, clock: 1, state: `{"user":"local"}`}})
	session.trackAwareness([]awarenessState{
		{clientID: 21, clock: 2, state: `{"user":"local"}`},
		{clientID: 22, clock: 8, state: `{"user":"relayed"}`},
	})

	removals := session.takeAwarenessRemovals()
	if len(removals) != 1 || removals[0].clientID != 21 || removals[0].clock != 3 {
		t.Fatalf("unexpected removals: %#v", removals)
	}
}
