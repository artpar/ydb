package ydb

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type YjsRoomName string

type room struct {
	mux           sync.Mutex
	offset        uint32
	lastActive    time.Time
	subCount      int32
	roomsessionid uint32
}

func (ydb *Ydb) newRoom() *room {
	return &room{
		roomsessionid: ydb.genUint32(),
		lastActive:    time.Now(),
	}
}

// updateRoom persists data to store, updates room offset, and broadcasts to subscribers.
func (ydb *Ydb) updateRoom(roomname YjsRoomName, session *session, bs []byte) {
	// Frame data for storage
	pendingWrite := &bytes.Buffer{}
	err := writePayload(pendingWrite, bs)
	if err != nil {
		log.Printf("Failed to create payload: %v", err)
		return
	}

	// Persist outside room mutex — per-room store mutex handles concurrency
	newOffset, err := ydb.store.Append(roomname, pendingWrite.Bytes())
	if err != nil {
		log.Printf("Failed to append to store: %v", err)
		return
	}

	// Update cached offset under room mutex (fast, no I/O)
	r := ydb.getOrCreateRoom(roomname)
	r.mux.Lock()
	r.offset = newOffset
	r.lastActive = time.Now()
	r.mux.Unlock()

	// Fan out to other subscribers
	ydb.broadcaster.Publish(roomname, session.sessionid, bs)
}

// subscribeRoom subscribes a session to a room, catching up from the store first.
func (ydb *Ydb) subscribeRoom(session *session, offset uint32) {
	roomname := session.roomname

	// Subscribe first — starts buffering broadcast messages immediately
	broadcastCh, err := ydb.broadcaster.Subscribe(roomname, session.sessionid)
	if err != nil {
		log.Printf("Failed to subscribe to broadcaster: %v", err)
		return
	}

	r := ydb.getOrCreateRoom(roomname)
	atomic.AddInt32(&r.subCount, 1)
	r.mux.Lock()
	r.lastActive = time.Now()
	r.mux.Unlock()

	// Catch-up from store
	data, currentOffset, err := ydb.store.ReadFrom(roomname, offset)
	if err != nil {
		log.Printf("Failed to read from store for room %s: %v", roomname, err)
	}

	if len(data) > 0 {
		dataReader := bytes.NewReader(data)
		for {
			payload, err := readPayload(dataReader)
			if err != nil {
				break
			}
			session.sendUpdate(roomname, payload, uint64(currentOffset))
		}
	}

	// Forward broadcast messages to session
	go func() {
		for msg := range broadcastCh {
			session.sendUpdate(roomname, msg, 0)
		}
		// Channel closed — unsubscribed
		atomic.AddInt32(&r.subCount, -1)
	}()
}
