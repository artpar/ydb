package ydb

import (
	"sync"

	"github.com/gorilla/websocket"
)

// serverConfirmation keeps track of confirmations created by the server.
type serverConfirmation struct {
	next         uint64
	nextClient   uint64
	roomsChanged map[YjsRoomName]uint64
}

func (serverConfirmation *serverConfirmation) createConfirmation() uint64 {
	conf := serverConfirmation.next
	serverConfirmation.next++
	return conf
}

func (serverConfirmation *serverConfirmation) clientConfirmed(confirmed uint64) {
	if serverConfirmation.nextClient <= confirmed {
		serverConfirmation.nextClient = confirmed + 1
		roomsChanged := serverConfirmation.roomsChanged
		serverConfirmation.roomsChanged = make(map[YjsRoomName]uint64, 1)
		for roomname, n := range roomsChanged {
			if n > confirmed {
				serverConfirmation.roomsChanged[roomname] = n
			}
		}
	}
}

// clientConfirmation keeps track of confirmation numbers received from the client.
type clientConfirmation struct {
	next  uint64
	confs map[uint64]struct{}
}

func (conf *clientConfirmation) serverConfirmed(confirmed uint64) (updated bool) {
	if conf.next == confirmed {
		conf.next = confirmed + 1
		if conf.confs != nil {
			_, ok := conf.confs[conf.next]
			for ok {
				_, ok = conf.confs[conf.next]
				conf.next++
			}
		}
		if conf.next != confirmed+1 {
			oldConfs := conf.confs
			newConfs := make(map[uint64]struct{}, 0)
			for n := range oldConfs {
				newConfs[n] = struct{}{}
			}
			if len(newConfs) > 0 {
				conf.confs = newConfs
			} else {
				conf.confs = nil
			}
		}
		return true
	}
	if conf.confs == nil {
		conf.confs = make(map[uint64]struct{}, 1)
	}
	conf.confs[confirmed] = struct{}{}
	return false
}

type session struct {
	mux                sync.Mutex
	conn               conn
	serverConfirmation serverConfirmation
	clientConfirmation clientConfirmation
	sessionid          uint64
	roomname           YjsRoomName
}

func newSession(sessionid uint64, roomname string) *session {
	return &session{
		sessionid: sessionid,
		roomname:  YjsRoomName(roomname),
	}
}

func (s *session) sendConfirmedByHost(roomname YjsRoomName, offset uint64) {
	s.send(createMessageConfirmedByHost(roomname, offset))
}

func (s *session) send(bs []byte) {
	s.mux.Lock()
	if s.conn != nil {
		pmessage, _ := websocket.NewPreparedMessage(websocket.BinaryMessage, bs)
		s.conn.WriteMessage(bs, pmessage)
	}
	s.mux.Unlock()
}

func (s *session) sendUpdate(roomname YjsRoomName, data []byte, offset uint64) {
	if len(data) > 0 {
		s.send(data)
	}
}

func (s *session) sendHostUnconfirmedByClient(clientConf uint64, offset uint64) {
	s.send(createMessageHostUnconfirmedByClient(clientConf, offset))
}

func (s *session) setConn(c conn) {
	s.mux.Lock()
	s.conn = c
	s.mux.Unlock()
}

func (s *session) removeConn(ydb *Ydb) {
	s.mux.Lock()
	s.conn = nil
	s.mux.Unlock()
	ydb.broadcaster.Unsubscribe(s.roomname, s.sessionid)
	ydb.removeSession(s.sessionid)
}
