package ydb

import (
	"context"
	"errors"
)

var ErrStoreWrite = errors.New("YDB store write failed")

type Store interface {
	Append(ctx context.Context, room YjsRoomName, data []byte) (newOffset uint32, err error)
	ReadFrom(room YjsRoomName, offset uint32) ([]byte, uint32, error)
	Size(room YjsRoomName) (uint32, error)
	SetInitialContent(room YjsRoomName, data []byte) error
}
