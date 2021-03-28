package ydb

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

const stdPerms = 0600

type roomUpdate struct {
	room     *room
	roomname YjsRoomName
}

type fswriter struct {
	queue chan roomUpdate
}

func (fswriter *fswriter) readRoomSize(filepath string) uint32 {
	fi, err := os.Stat(filepath)
	switch err.(type) {
	case nil:
	case *os.PathError:
		return 0
	default:
		panic("unexpected error while reading file stats")
	}
	return uint32(fi.Size())
}

func (fswriter *fswriter) registerRoomUpdate(room *room, roomname YjsRoomName) {
	fswriter.queue <- roomUpdate{room, roomname}
}

func (fswriter *fswriter) startWriteTask(dir string) {
	for {
		writeTask := <-fswriter.queue
		room := writeTask.room
		roomname := writeTask.roomname
		writeFilepath := fmt.Sprintf("%s/%s", dir, string(roomname))
		time.Sleep(time.Millisecond * 800)
		room.mux.Lock()
		debug("fswriter: created room lock")
		pendingWrites := room.pendingWrites
		dataAvailable := false
		if len(pendingWrites) > 0 {
			// New data is available.
			dataAvailable = true
			// This goroutine will save dataAvailable and confirm pending confirmations
			room.pendingWrites = nil
		}
		for _, sub := range room.pendingSubs {
			if !room.hasSession(sub.session) {
				f, _ := os.OpenFile(writeFilepath, os.O_RDONLY|os.O_CREATE, stdPerms)
				if sub.offset > 0 {
					f.Seek(int64(sub.offset), 0)
				}
				data, _ := ioutil.ReadAll(f)
				data = append(data, pendingWrites...)
				//confirmedOffset := uint64(sub.offset) + uint64(len(data))
				// TODO: combine sub and update here
				//sub.session.sendUpdate(roomname, data, confirmedOffset)
				//sub.session.sendConfirmedByHost(roomname, confirmedOffset)
				room.subs = append(room.subs, sub.session)
				f.Close()
			}
		}
		room.pendingSubs = nil
		room.registered = false
		if dataAvailable {
			debug("fswriter: enter dataAvailable - write file")
			f, err := os.OpenFile(writeFilepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, stdPerms)
			if err != nil {
				panic(err)
			}
			debug("fswriter: opened file")

			if _, err = f.Write(pendingWrites); err != nil {
				panic(err)
			}
			debug("fswriter: writing file")
			f.Close()
			debug("fswriter: closed file")
			// confirm after we can assure that data has been written
			//for _, sub := range room.subs {
			//	sub.sendConfirmedByHost(roomname, uint64(room.offset))
			//}
			debug("fswriter: left dataAvailable - sent confirmedByHost")
		}
		room.mux.Unlock()
		debug("fswriter: removed lock")
	}
}
