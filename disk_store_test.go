package ydb

import (
	"fmt"
	"sync"
	"testing"
)

func TestDiskStoreAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	room := YjsRoomName("testroom")
	data := []byte("hello world")

	newOffset, err := store.Append(room, data)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if newOffset != uint32(len(data)) {
		t.Fatalf("expected offset %d, got %d", len(data), newOffset)
	}

	readData, readOffset, err := store.ReadFrom(room, 0)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("expected %q, got %q", data, readData)
	}
	if readOffset != newOffset {
		t.Fatalf("expected offset %d, got %d", newOffset, readOffset)
	}
}

func TestDiskStoreReadFromOffset(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	room := YjsRoomName("testroom")
	store.Append(room, []byte("AAABBB"))

	readData, _, err := store.ReadFrom(room, 3)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if string(readData) != "BBB" {
		t.Fatalf("expected %q, got %q", "BBB", readData)
	}
}

func TestDiskStoreSizeTracking(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	room := YjsRoomName("testroom")

	size, _ := store.Size(room)
	if size != 0 {
		t.Fatalf("expected size 0 for new room, got %d", size)
	}

	store.Append(room, []byte("abc"))
	size, _ = store.Size(room)
	if size != 3 {
		t.Fatalf("expected size 3, got %d", size)
	}

	store.Append(room, []byte("defgh"))
	size, _ = store.Size(room)
	if size != 8 {
		t.Fatalf("expected size 8, got %d", size)
	}
}

func TestDiskStoreMaxRoomSize(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir, WithMaxRoomSize(10))

	room := YjsRoomName("testroom")

	_, err := store.Append(room, []byte("12345"))
	if err != nil {
		t.Fatalf("first append should succeed: %v", err)
	}

	_, err = store.Append(room, []byte("123456"))
	if err == nil {
		t.Fatalf("second append should fail (exceeds max size)")
	}

	// Exactly at limit should succeed
	_, err = store.Append(room, []byte("12345"))
	if err != nil {
		t.Fatalf("append to exactly max size should succeed: %v", err)
	}
}

func TestDiskStoreMaxRoomSizeFirstWrite(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir, WithMaxRoomSize(10))

	room := YjsRoomName("newroom")

	// First write to a non-existent file should also be capped
	_, err := store.Append(room, []byte("this-exceeds-ten-bytes"))
	if err == nil {
		t.Fatalf("first write exceeding max size should fail")
	}

	// First write within limit should succeed
	_, err = store.Append(room, []byte("ok"))
	if err != nil {
		t.Fatalf("first write within limit should succeed: %v", err)
	}
}

func TestDiskStoreConcurrentSameRoom(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	room := YjsRoomName("testroom")
	var wg sync.WaitGroup
	n := 100

	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			store.Append(room, []byte(fmt.Sprintf("data-%03d|", i)))
		}(i)
	}
	wg.Wait()

	size, _ := store.Size(room)
	// Each entry is "data-NNN|" = 9 bytes
	if size != uint32(n*9) {
		t.Fatalf("expected size %d, got %d", n*9, size)
	}

	data, _, _ := store.ReadFrom(room, 0)
	if len(data) != n*9 {
		t.Fatalf("expected %d bytes, got %d", n*9, len(data))
	}
}

func TestDiskStoreConcurrentDifferentRooms(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	var wg sync.WaitGroup
	n := 100

	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			room := YjsRoomName(fmt.Sprintf("room-%d", i))
			data := []byte(fmt.Sprintf("data-%d", i))
			store.Append(room, data)
		}(i)
	}
	wg.Wait()

	for i := range n {
		room := YjsRoomName(fmt.Sprintf("room-%d", i))
		data, _, err := store.ReadFrom(room, 0)
		if err != nil {
			t.Fatalf("ReadFrom room-%d failed: %v", i, err)
		}
		expected := fmt.Sprintf("data-%d", i)
		if string(data) != expected {
			t.Fatalf("room-%d: expected %q, got %q", i, expected, data)
		}
	}
}

func TestDiskStoreSetInitialContent(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	room := YjsRoomName("testroom")
	initial := []byte("initial content")

	err := store.SetInitialContent(room, initial)
	if err != nil {
		t.Fatalf("SetInitialContent failed: %v", err)
	}

	data, offset, err := store.ReadFrom(room, 0)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if string(data) != string(initial) {
		t.Fatalf("expected %q, got %q", initial, data)
	}
	if offset != uint32(len(initial)) {
		t.Fatalf("expected offset %d, got %d", len(initial), offset)
	}
}

func TestDiskStoreInitialContentProvider(t *testing.T) {
	dir := t.TempDir()
	providerCalled := false
	store := NewDiskStore(dir, WithInitialContentProvider(func(roomName string) []byte {
		providerCalled = true
		return []byte("provided-" + roomName)
	}))

	room := YjsRoomName("myroom")

	// First access triggers the provider
	data, _, err := store.ReadFrom(room, 0)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if !providerCalled {
		t.Fatalf("expected provider to be called")
	}
	if string(data) != "provided-myroom" {
		t.Fatalf("expected %q, got %q", "provided-myroom", data)
	}

	// Append after initial content
	store.Append(room, []byte("-appended"))
	data, _, _ = store.ReadFrom(room, 0)
	if string(data) != "provided-myroom-appended" {
		t.Fatalf("expected %q, got %q", "provided-myroom-appended", data)
	}
}

func TestDiskStoreReadNonExistent(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskStore(dir)

	room := YjsRoomName("nonexistent")
	data, offset, err := store.ReadFrom(room, 0)
	if err != nil {
		t.Fatalf("ReadFrom non-existent should not error: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}
}
