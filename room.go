package ydb

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"sync"
)

type YjsRoomName string

type pendingWrite struct {
	data    []byte
	session *session
	conf    uint64
}

type room struct {
	mux           sync.Mutex
	registered    bool
	pendingWrites []byte
	subs          []*session
	pendingSubs   []pendingSub
	roomsessionid uint32
	offset        uint32
}

func (ydb *Ydb) newRoom() *room {
	return &room{
		subs:          nil,
		roomsessionid: ydb.genUint32(),
		offset:        0, // TODO: all available rooms should be initialized with offset when Ydb initializes
	}
}

func (ydb *Ydb) modifyRoom(roomname YjsRoomName, f func(room *room) (modified bool), tx *sql.Tx) {
	room := ydb.GetYjsRoom(roomname, tx)
	var register bool
	room.mux.Lock()
	// try to clean up subs
	needsCleanup := false
	for _, s := range room.subs {
		if s.conn == nil {
			needsCleanup = true
			break
		}
	}
	if needsCleanup {
		var newSubs []*session
		for _, s := range room.subs {
			if s.conn != nil {
				newSubs = append(newSubs, s)
			}
		}
		room.subs = newSubs
	}

	modified := f(room)
	if room.registered == false && modified {
		register = true
		room.registered = true
	}
	room.mux.Unlock()
	if register {
		ydb.documentProvider.RegisterRoomUpdate(room, roomname, tx)
	}
}

// update in-memory buffer of writable data. Registers in fswriter if new data is available.
// Writes to buffer until fswriter owns the buffer.
func (ydb *Ydb) updateRoom(roomname YjsRoomName, session *session, bs []byte, tx *sql.Tx) {
	debug("trying to update room")
	ydb.modifyRoom(roomname, func(room *room) bool {
		debug("updating room")
		//fmt.Printf("Payload: %v - %v", bs, base64.StdEncoding.EncodeToString(bs))

		pendingWrite := &bytes.Buffer{}
		err := writePayload(pendingWrite, bs)
		if err != nil {
			log.Printf("Failed to create payload: %v", err)
		}

		room.pendingWrites = append(room.pendingWrites, pendingWrite.Bytes()...)

		room.offset += uint32(len(bs))
		debug(fmt.Sprintf("updating room .. number of subs: %d", len(room.subs)))
		for _, s := range room.subs {
			if s != session {
				s.sendUpdate(roomname, bs, uint64(room.offset))
			}
		}
		debug("updating room .. wrote update to all sessions but sender")
		//session.sendHostUnconfirmedByClient(clientConf, uint64(room.offset))
		debug("updating room .. sent conf to client")
		return true
	}, tx)
	debug("done updating room")
}

type pendingSub struct {
	session *session
	offset  uint32
}

func (room *room) hasSession(session *session) bool {
	for _, s := range room.subs {
		if s == session {
			return true
		}
	}
	return false
}

func (ydb *Ydb) subscribeRoom(session *session, offset uint32, tx *sql.Tx) {
	ydb.modifyRoom(session.roomname, func(room *room) bool {
		if !room.hasSession(session) {
			if room.offset != offset {
				room.pendingSubs = append(room.pendingSubs, pendingSub{session, offset})
				return true
			}
			room.subs = append(room.subs, session)
			// session.sendConfirmedByHost(YjsRoomName, uint64(offset))
		}
		return false // whether room data needs to access fswriter
	}, tx)
}
