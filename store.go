package ydb

type Store interface {
	Append(room YjsRoomName, data []byte) (newOffset uint32, err error)
	ReadFrom(room YjsRoomName, offset uint32) ([]byte, uint32, error)
	Size(room YjsRoomName) (uint32, error)
	SetInitialContent(room YjsRoomName, data []byte) error
}
