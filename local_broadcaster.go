package ydb

import "sync"

type LocalBroadcaster struct {
	mu      sync.RWMutex
	rooms   map[YjsRoomName]map[uint64]chan []byte
	bufSize int
}

func NewLocalBroadcaster(bufferSize int) Broadcaster {
	return &LocalBroadcaster{
		rooms:   make(map[YjsRoomName]map[uint64]chan []byte),
		bufSize: bufferSize,
	}
}

func (lb *LocalBroadcaster) Publish(room YjsRoomName, senderSessionID uint64, data []byte) {
	lb.mu.RLock()
	subs := lb.rooms[room]
	if subs == nil {
		lb.mu.RUnlock()
		return
	}

	msg := make([]byte, len(data))
	copy(msg, data)

	for sid, ch := range subs {
		if sid != senderSessionID {
			select {
			case ch <- msg:
			default:
			}
		}
	}
	lb.mu.RUnlock()
}

func (lb *LocalBroadcaster) Subscribe(room YjsRoomName, sessionID uint64) (<-chan []byte, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	subs, ok := lb.rooms[room]
	if !ok {
		subs = make(map[uint64]chan []byte)
		lb.rooms[room] = subs
	}

	ch := make(chan []byte, lb.bufSize)
	subs[sessionID] = ch
	return ch, nil
}

func (lb *LocalBroadcaster) Unsubscribe(room YjsRoomName, sessionID uint64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	subs, ok := lb.rooms[room]
	if !ok {
		return
	}

	ch, ok := subs[sessionID]
	if ok {
		close(ch)
		delete(subs, sessionID)
	}
	if len(subs) == 0 {
		delete(lb.rooms, room)
	}
}
