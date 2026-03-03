package ydb

type Broadcaster interface {
	Publish(room YjsRoomName, senderSessionID uint64, data []byte)
	Subscribe(room YjsRoomName, sessionID uint64) (<-chan []byte, error)
	Unsubscribe(room YjsRoomName, sessionID uint64)
}
