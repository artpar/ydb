package ydb

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestStressManyClientsOneRoom(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ts := newTestServer(t)
	roomname := "stress-one-room"
	numClients := 100
	updatesPerClient := 10

	clients := make([]*testWsClient, numClients)
	for i := range numClients {
		clients[i] = ts.dial(t, roomname)
	}
	// Let all subscriptions settle
	time.Sleep(200 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(numClients)
	for i := range numClients {
		go func(clientIdx int) {
			defer wg.Done()
			for j := range updatesPerClient {
				payload := []byte(fmt.Sprintf("c%d-u%d", clientIdx, j))
				clients[clientIdx].sendSyncUpdate(payload)
				time.Sleep(time.Millisecond) // slight spacing
			}
		}(i)
	}
	wg.Wait()

	totalUpdates := numClients * updatesPerClient

	// Wait for all updates to be persisted in store
	waitFor(t, 10*time.Second, func() bool {
		data, _, _ := ts.store.ReadFrom(YjsRoomName(roomname), 0)
		if data == nil {
			return false
		}
		// Count frames in store
		reader := bytes.NewReader(data)
		count := 0
		for {
			_, err := readPayload(reader)
			if err != nil {
				break
			}
			count++
		}
		return count >= totalUpdates
	})
}

func TestStressManyRooms(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ts := newTestServer(t)
	numRooms := 50
	clientsPerRoom := 5
	updatesPerClient := 10

	var wg sync.WaitGroup
	for r := range numRooms {
		roomname := fmt.Sprintf("stress-room-%d", r)
		clients := make([]*testWsClient, clientsPerRoom)
		for c := range clientsPerRoom {
			clients[c] = ts.dial(t, roomname)
		}
		time.Sleep(20 * time.Millisecond)

		for c := range clientsPerRoom {
			wg.Add(1)
			go func(client *testWsClient, roomIdx, clientIdx int) {
				defer wg.Done()
				for u := range updatesPerClient {
					payload := []byte(fmt.Sprintf("r%d-c%d-u%d", roomIdx, clientIdx, u))
					client.sendSyncUpdate(payload)
					time.Sleep(time.Millisecond)
				}
			}(clients[c], r, c)
		}
	}
	wg.Wait()

	// Verify each room has the right number of updates
	for r := range numRooms {
		roomname := YjsRoomName(fmt.Sprintf("stress-room-%d", r))
		expected := clientsPerRoom * updatesPerClient

		waitFor(t, 10*time.Second, func() bool {
			data, _, _ := ts.store.ReadFrom(roomname, 0)
			if data == nil {
				return false
			}
			reader := bytes.NewReader(data)
			count := 0
			for {
				_, err := readPayload(reader)
				if err != nil {
					break
				}
				count++
			}
			return count >= expected
		})
	}
}

func TestStressRapidConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ts := newTestServer(t)

	goroutinesBefore := runtime.NumGoroutine()
	cycles := 200

	for i := range cycles {
		roomname := fmt.Sprintf("rapid-%d", i%10) // reuse some rooms
		c := ts.dial(t, roomname)
		time.Sleep(2 * time.Millisecond)
		c.close()
	}

	// Wait for goroutines to settle
	time.Sleep(2 * time.Second)

	goroutinesAfter := runtime.NumGoroutine()
	// Allow some slack for background goroutines
	leaked := goroutinesAfter - goroutinesBefore
	if leaked > 50 {
		t.Fatalf("potential goroutine leak: before=%d after=%d leaked=%d",
			goroutinesBefore, goroutinesAfter, leaked)
	}
}

func TestStressLargePayloads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ts := newTestServer(t)
	roomname := "large-payload"

	clientA := ts.dial(t, roomname)
	clientB := ts.dial(t, roomname)
	time.Sleep(100 * time.Millisecond)

	// 1MB payload
	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	clientA.sendSyncUpdate(payload)

	// Client B should receive it
	msg, ok := clientB.recv(5 * time.Second)
	if !ok {
		t.Fatalf("client B timed out waiting for large payload")
	}

	_, innerPayload, err := parseSyncMessage(msg)
	if err != nil {
		t.Fatalf("parseSyncMessage failed: %v", err)
	}
	if !bytes.Equal(innerPayload, payload) {
		t.Fatalf("large payload mismatch: got len %d, want len %d", len(innerPayload), len(payload))
	}
}

// --- Benchmarks ---

func BenchmarkDiskStoreAppend(b *testing.B) {
	dir := b.TempDir()
	store := NewDiskStore(dir)
	room := YjsRoomName("bench-room")
	data := []byte("benchmark-payload-data-1234567890")

	b.ResetTimer()
	for i := range b.N {
		_ = i
		store.Append(room, data)
	}
}

func BenchmarkDiskStoreAppendParallel(b *testing.B) {
	dir := b.TempDir()
	store := NewDiskStore(dir)
	data := []byte("benchmark-payload-data-1234567890")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			room := YjsRoomName("bench-room-" + strconv.Itoa(r.Intn(100)))
			store.Append(room, data)
		}
	})
}

func BenchmarkBroadcasterFanout(b *testing.B) {
	lb := NewLocalBroadcaster(1024)
	room := YjsRoomName("bench-room")
	numSubs := 100

	for i := range numSubs {
		lb.Subscribe(room, uint64(i+1))
	}

	senderID := uint64(9999)
	data := []byte("benchmark-broadcast-payload")

	b.ResetTimer()
	for range b.N {
		lb.Publish(room, senderID, data)
	}
}

func BenchmarkGetOrCreateRoom(b *testing.B) {
	store := newMemoryStore()
	broadcaster := NewLocalBroadcaster(64)
	cfg := DefaultConfig()
	ydbInstance := InitYdb(store, broadcaster, cfg)
	defer ydbInstance.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			name := YjsRoomName(strconv.Itoa(r.Intn(1000)))
			ydbInstance.getOrCreateRoom(name)
		}
	})
}
