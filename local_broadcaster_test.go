package ydb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBroadcasterPublishReceive(t *testing.T) {
	lb := NewLocalBroadcaster(16)
	room := YjsRoomName("testroom")

	ch, err := lb.Subscribe(room, 1)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	lb.Publish(room, 99, []byte("hello"))

	select {
	case msg := <-ch:
		if string(msg) != "hello" {
			t.Fatalf("expected %q, got %q", "hello", msg)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for message")
	}
}

func TestBroadcasterSenderExcluded(t *testing.T) {
	lb := NewLocalBroadcaster(16)
	room := YjsRoomName("testroom")

	senderID := uint64(1)
	ch, _ := lb.Subscribe(room, senderID)

	lb.Publish(room, senderID, []byte("self-message"))

	select {
	case msg := <-ch:
		t.Fatalf("sender should not receive own message, got %q", msg)
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestBroadcasterMultipleSubscribers(t *testing.T) {
	lb := NewLocalBroadcaster(16)
	room := YjsRoomName("testroom")

	n := 10
	channels := make([]<-chan []byte, n)
	for i := range n {
		ch, _ := lb.Subscribe(room, uint64(i+1))
		channels[i] = ch
	}

	senderID := uint64(99)
	lb.Publish(room, senderID, []byte("broadcast"))

	for i, ch := range channels {
		select {
		case msg := <-ch:
			if string(msg) != "broadcast" {
				t.Fatalf("subscriber %d: expected %q, got %q", i, "broadcast", msg)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out", i)
		}
	}
}

func TestBroadcasterUnsubscribe(t *testing.T) {
	lb := NewLocalBroadcaster(16)
	room := YjsRoomName("testroom")

	ch, _ := lb.Subscribe(room, 1)
	lb.Unsubscribe(room, 1)

	// Channel should be closed
	_, ok := <-ch
	if ok {
		t.Fatalf("expected channel to be closed after unsubscribe")
	}
}

func TestBroadcasterEmptyRoomPublish(t *testing.T) {
	lb := NewLocalBroadcaster(16)
	room := YjsRoomName("emptyroom")

	// Should not panic
	lb.Publish(room, 1, []byte("nobody home"))
}

func TestBroadcasterSlowSubscriberDrop(t *testing.T) {
	lb := NewLocalBroadcaster(2) // buffer of 2
	room := YjsRoomName("testroom")

	ch, _ := lb.Subscribe(room, 1)

	// Fill the buffer
	lb.Publish(room, 99, []byte("msg1"))
	lb.Publish(room, 99, []byte("msg2"))

	// This should be dropped (non-blocking)
	lb.Publish(room, 99, []byte("msg3"))

	msg1 := <-ch
	msg2 := <-ch
	if string(msg1) != "msg1" || string(msg2) != "msg2" {
		t.Fatalf("expected msg1 and msg2, got %q and %q", msg1, msg2)
	}

	select {
	case msg := <-ch:
		t.Fatalf("expected no more messages, got %q", msg)
	case <-time.After(50 * time.Millisecond):
		// expected — msg3 was dropped
	}
}

func TestBroadcasterConcurrentPublish(t *testing.T) {
	lb := NewLocalBroadcaster(1000)
	room := YjsRoomName("testroom")

	ch, _ := lb.Subscribe(room, 1)

	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			lb.Publish(room, uint64(i+2), []byte(fmt.Sprintf("msg-%d", i)))
		}(i)
	}
	wg.Wait()

	// Drain and count
	count := 0
	for {
		select {
		case <-ch:
			count++
		case <-time.After(100 * time.Millisecond):
			goto done
		}
	}
done:
	if count != n {
		t.Fatalf("expected %d messages, got %d", n, count)
	}
}

func TestBroadcasterCrossRoomIsolation(t *testing.T) {
	lb := NewLocalBroadcaster(16)
	roomA := YjsRoomName("roomA")
	roomB := YjsRoomName("roomB")

	chA, _ := lb.Subscribe(roomA, 1)
	chB, _ := lb.Subscribe(roomB, 2)

	lb.Publish(roomA, 99, []byte("for-A"))

	select {
	case msg := <-chA:
		if string(msg) != "for-A" {
			t.Fatalf("roomA: expected %q, got %q", "for-A", msg)
		}
	case <-time.After(time.Second):
		t.Fatalf("roomA: timed out")
	}

	select {
	case msg := <-chB:
		t.Fatalf("roomB should not receive roomA message, got %q", msg)
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}
