package ydb

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Ydb maintains rooms and connections
type Ydb struct {
	roomsMux    sync.RWMutex
	rooms       map[YjsRoomName]*room
	sessionsMux sync.Mutex
	sessions    map[uint64]*session
	seed        *rand.Rand
	seedMux     sync.Mutex
	store       Store
	broadcaster Broadcaster
	cfg         Config
	done        chan struct{}
}

func (ydb *Ydb) genUint32() uint32 {
	ydb.seedMux.Lock()
	n := ydb.seed.Uint32()
	ydb.seedMux.Unlock()
	return n
}

func (ydb *Ydb) genUint64() uint64 {
	ydb.seedMux.Lock()
	n := ydb.seed.Uint64()
	ydb.seedMux.Unlock()
	return n
}

func InitYdb(store Store, broadcaster Broadcaster, cfgs ...Config) *Ydb {
	cfg := DefaultConfig()
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	ydb := &Ydb{
		rooms:       make(map[YjsRoomName]*room, 1000),
		sessions:    make(map[uint64]*session),
		seed:        rand.New(rand.NewSource(time.Now().UnixNano())),
		store:       store,
		broadcaster: broadcaster,
		cfg:         cfg,
		done:        make(chan struct{}),
	}
	go ydb.roomReaper()
	return ydb
}

func (ydb *Ydb) Close() {
	close(ydb.done)
}

// getOrCreateRoom returns existing room or creates a new one, reading offset from store.
func (ydb *Ydb) getOrCreateRoom(name YjsRoomName) *room {
	ydb.roomsMux.RLock()
	r := ydb.rooms[name]
	ydb.roomsMux.RUnlock()
	if r == nil {
		ydb.roomsMux.Lock()
		r = ydb.rooms[name]
		if r == nil {
			r = ydb.newRoom()
			ydb.rooms[name] = r
			ydb.roomsMux.Unlock()
			size, _ := ydb.store.Size(name)
			r.mux.Lock()
			r.offset = size
			r.mux.Unlock()
		} else {
			ydb.roomsMux.Unlock()
		}
	}
	return r
}

func (ydb *Ydb) getSession(sessionid uint64) *session {
	ydb.sessionsMux.Lock()
	defer ydb.sessionsMux.Unlock()
	return ydb.sessions[sessionid]
}

func (ydb *Ydb) createSession(roomname string) *session {
	ydb.sessionsMux.Lock()
	sessionid := ydb.genUint64()
	if _, ok := ydb.sessions[sessionid]; ok {
		panic("Generated the same session id twice! (this is a security vulnerability)")
	}
	s := newSession(sessionid, roomname)
	ydb.sessions[sessionid] = s
	ydb.sessionsMux.Unlock()
	return s
}

func (ydb *Ydb) removeSession(sessionid uint64) {
	ydb.sessionsMux.Lock()
	delete(ydb.sessions, sessionid)
	ydb.sessionsMux.Unlock()
}

func (ydb *Ydb) roomReaper() {
	ticker := time.NewTicker(ydb.cfg.RoomReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ydb.done:
			return
		case <-ticker.C:
			ydb.reapIdleRooms()
		}
	}
}

func (ydb *Ydb) reapIdleRooms() {
	now := time.Now()
	ydb.roomsMux.RLock()
	var toRemove []YjsRoomName
	for name, r := range ydb.rooms {
		if atomic.LoadInt32(&r.subCount) == 0 {
			r.mux.Lock()
			idle := now.Sub(r.lastActive) > ydb.cfg.RoomIdleTimeout
			r.mux.Unlock()
			if idle {
				toRemove = append(toRemove, name)
			}
		}
	}
	ydb.roomsMux.RUnlock()

	if len(toRemove) > 0 {
		ydb.roomsMux.Lock()
		for _, name := range toRemove {
			r := ydb.rooms[name]
			if r != nil && atomic.LoadInt32(&r.subCount) == 0 {
				delete(ydb.rooms, name)
			}
		}
		ydb.roomsMux.Unlock()
	}
}
