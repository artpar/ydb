package ydb

import (
	"sync"

	"github.com/gorilla/websocket"
)

// serverConfirmation keeps track of confirmations created by the server.
// serverConfirmation.next is attached to data sent from the server to a client (via some conn).
// When the client confirms a serverConfirmation number, it assures that it consumed and persisted the sent data data.
// Hence the server does not have to keep track of roomsChanged since last confirmed message.
type serverConfirmation struct {
	// current confirmation number
	next uint64
	// next expected confirmation from client
	nextClient uint64
	// rooms changed since last confirmation.
	roomsChanged map[YjsRoomName]uint64
}

func (serverConfirmation *serverConfirmation) createConfirmation() uint64 {
	conf := serverConfirmation.next
	serverConfirmation.next++
	return conf
}

// client confirmed that it received and persisted data.
func (serverConfirmation *serverConfirmation) clientConfirmed(confirmed uint64) {
	if serverConfirmation.nextClient <= confirmed {
		serverConfirmation.nextClient = confirmed + 1
		// recreate a new roomsChanged map to assure that memory does not grow
		roomsChanged := serverConfirmation.roomsChanged
		serverConfirmation.roomsChanged = make(map[YjsRoomName]uint64, 1)
		// re-insert all rooms that are not yet confirmed
		for roomname, n := range roomsChanged {
			if n > confirmed {
				serverConfirmation.roomsChanged[roomname] = n
			}
		}
	}
}

// clientConfirmation keeps track of confirmation numbers received from the client.
// ydb confirms messages when they are consumed and persisted on the disk.
// The client will receive confirmations in-order. But since the process of persisting
// data is asynchronous, it may happen that confirmations are created out-of-order.
// I.e. The client wrote data to room "a" (confirmation 0) then "b" (confirmation 1).
// But the server creates confirmation 1, then 0. We keep track of the confirmations in a map.
type clientConfirmation struct {
	// next expected confirmation
	next uint64
	// set of out-of-order created confirmations (none of them is `next`)
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
			// conf updated based on confs
			// conf.confs needs to be updated
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
	mux sync.Mutex
	// currently active connection
	conn conn
	// set of all conns
	conns []conn
	// client confirming messages to server
	serverConfirmation serverConfirmation
	// server confirming messages to client
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
	/* TODO: use the following for UnconfirmedHostByClient
	s.mux.Lock()
	if s.clientConfirmation.serverConfirmed(confirmation) {
		confMessage := createMessageConfirmation(confirmation)
		pmessage, err := websocket.NewPreparedMessage(websocket.BinaryMessage, confMessage)
		if err != nil {
			fmt.Printf("ydb error creating formatted message: %s", err)
			return
		}
		if s.conn != nil {
			s.conn.WriteMessage(confMessage, pmessage)
		}
	}
	s.mux.Unlock()
	*/
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

func (s *session) add(conn conn) {
	s.mux.Lock()
	s.conns = append(s.conns, conn)
	if s.conn == nil {
		s.conn = conn
	}
	s.mux.Unlock()
}

func (s *session) removeConn(c conn, ydb *Ydb) {
	s.mux.Lock()
	var newConns []conn
	for _, conn := range s.conns {
		if c != conn {
			newConns = append(newConns, conn)
		}
	}
	s.conns = newConns
	if s.conn == c {
		if s.conns != nil { // conns is not empty
			s.conn = s.conns[0]
		} else {
			s.conn = nil
		}
	}
	if s.conn == nil {
		ydb.removeSession(s.sessionid)
	}
	s.mux.Unlock()
}

//
//func (s *session) createDeleteSetFromStructStore() interface{} {
//
//}
//
//func (s *session) writeDeleteSet(encoder *bytes.Buffer, store interface{}) {
//
//}
