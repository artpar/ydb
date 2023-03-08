package ydb

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Ydb maintains rooms and connections
type Ydb struct {
	roomsMux sync.RWMutex
	rooms    map[YjsRoomName]*room
	// TODO: use guid instead of uint64
	sessionsMux      sync.Mutex
	sessions         map[uint64]*session
	seed             *rand.Rand
	seedMux          sync.Mutex
	documentProvider DocumentProvider
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

func InitYdb(documentProvider DocumentProvider) *Ydb {
	// remember to update UnsafeClearAllYdbContent when updating here
	ydb := Ydb{
		rooms:    make(map[YjsRoomName]*room, 1000),
		sessions: make(map[uint64]*session),
		//fswriter:         newFSWriter(dir, 1000, 10), // TODO: have command line arguments for this
		documentProvider: documentProvider, // TODO: have command line arguments for this
		seed:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return &ydb
}

// GetYjsRoom from the global ydb instance. safe for parallel access.
func (ydb *Ydb) GetYjsRoom(name YjsRoomName, tx *sqlx.Tx) *room {
	ydb.roomsMux.RLock()
	r := ydb.rooms[name]
	ydb.roomsMux.RUnlock()
	if r == nil {
		ydb.roomsMux.Lock()
		r = ydb.rooms[name]
		if r == nil {
			r = ydb.newRoom()
			ydb.rooms[name] = r
			r.mux.Lock()
			ydb.roomsMux.Unlock()
			// read room offset..
			r.offset = ydb.documentProvider.ReadRoomSize(name, tx)
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

func (ydb *Ydb) createSession(roomname string, tx *sqlx.Tx) (s *session) {
	ydb.sessionsMux.Lock()
	sessionid := ydb.genUint64()
	if _, ok := ydb.sessions[sessionid]; ok {
		panic("Generated the same session id twice! (this is a security vulnerability)")
	}
	s = newSession(sessionid, roomname)
	_ = ydb.documentProvider.GetDocument(YjsRoomName(roomname), tx)
	ydb.sessions[sessionid] = s
	ydb.sessionsMux.Unlock()
	return s
}

func (ydb *Ydb) removeSession(sessionid uint64) (err error) {
	ydb.sessionsMux.Lock()
	session, ok := ydb.sessions[sessionid]
	if !ok {
		err = fmt.Errorf("tried to close session %d, but session does not exist", sessionid)
	} else if len(session.conns) > 0 || session.conn != nil {
		err = errors.New("Cannot close this session because conns are still using it")
	} else {
		delete(ydb.sessions, sessionid)
	}
	ydb.sessionsMux.Unlock()
	return
}

// TODO: refactor/remove..
func removeFSWriteDirContent(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	d.Chmod(0777)
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	debug("read dir names")
	for _, name := range names {
		os.Chmod(filepath.Join(dir, name), 0777)
		err = os.Remove(filepath.Join(dir, name))
		debug("removed a file")
		if err != nil {
			return err
		}
	}
	return nil
}
