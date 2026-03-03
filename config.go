package ydb

import "time"

type Config struct {
	SendBufferSize   int
	MaxMessageSize   int64
	MaxRoomSize      uint32
	BroadcastBuffer  int
	RoomIdleTimeout  time.Duration
	RoomReapInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		SendBufferSize:   256,
		MaxMessageSize:   10 * 1024 * 1024,
		MaxRoomSize:      50 * 1024 * 1024,
		BroadcastBuffer:  64,
		RoomIdleTimeout:  5 * time.Minute,
		RoomReapInterval: 1 * time.Minute,
	}
}
